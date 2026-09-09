//! MPEG-TS → MP4 remuxing for HLS video backups.
//!
//! Bluesky's video CDN serves HLS streams whose segments are MPEG-TS
//! (H.264 + AAC ADTS) rather than fMP4. Concatenating those segments
//! yields a valid `.ts` stream but not something a browser `<video>`
//! element can play. This module turns the concatenated TS bytes into a
//! self-contained, moov-first progressive MP4 (the same reassembly the
//! fMP4 path gets for free), so archived videos stay playable offline
//! without an HLS client or transcoder.
//!
//! Scope is deliberately narrow: the streams Bluesky actually serves —
//! one H.264 video track, zero or one AAC (ADTS) audio track,
//! unencrypted. Anything outside that fails with [`RemuxError`] and the
//! caller keeps the raw TS bytes under an honest `video/mp2t` label.

#[derive(Debug, thiserror::Error)]
pub enum RemuxError {
    #[error("not an MPEG-TS stream")]
    NotTs,
    #[error("TS stream has no usable elementary streams (PAT/PMT)")]
    NoStreams,
    #[error("TS stream carries no video track")]
    NoVideo,
    #[error("no SPS/PPS seen in the video stream")]
    MissingParameterSets,
}

const TS_PACKET: usize = 188;
const VIDEO_TIMESCALE: u32 = 90_000;
const AAC_SAMPLES_PER_FRAME: u32 = 1024;

// ---------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------

/// Whether `bytes` look like an MPEG-TS stream: the sync byte repeats at
/// the 188-byte packet stride. Checked before remuxing so fMP4-based HLS
/// streams (init segment + `.m4s` parts) keep using plain concatenation.
pub fn looks_like_mpeg_ts(bytes: &[u8]) -> bool {
    bytes.len() >= 3 * TS_PACKET
        && bytes[0] == 0x47
        && bytes[TS_PACKET] == 0x47
        && bytes[2 * TS_PACKET] == 0x47
}

/// Remuxes concatenated MPEG-TS bytes (H.264 video + optional AAC audio,
/// as served by Bluesky's HLS segments) into a progressive MP4 file.
pub fn remux_ts_to_mp4(ts: &[u8]) -> Result<Vec<u8>, RemuxError> {
    if !looks_like_mpeg_ts(ts) {
        return Err(RemuxError::NotTs);
    }
    let demuxed = Demuxer::run(ts)?;
    mux(&demuxed)
}

// ---------------------------------------------------------------------
// Demuxer
// ---------------------------------------------------------------------

/// One track sample: a video access unit or an AAC frame.
struct Sample {
    /// Presentation timestamp, 90 kHz ticks.
    pts: u64,
    /// Decode timestamp (== pts when the PES carries none).
    dts: u64,
    /// Video: AVCC-style NALUs (4-byte big-endian length prefixes,
    /// parameter sets stripped). Audio: one raw AAC frame.
    data: Vec<u8>,
    /// Video only: contains an IDR NAL (a sync sample).
    idr: bool,
}

struct Demuxed {
    sps: Vec<u8>,
    pps: Vec<u8>,
    width: u16,
    height: u16,
    video: Vec<Sample>,
    audio: Vec<Sample>,
    /// AudioSpecificConfig for the mp4a esds record.
    asc: Vec<u8>,
    audio_channels: u8,
    audio_sample_rate: u32,
}

/// Accumulates PES packets for one elementary PID across TS packets.
#[derive(Default)]
struct PesAssembler {
    buf: Vec<u8>,
    /// PES_packet_length when bounded (audio), else 0 (video: runs to
    /// the next PUSI on the same PID).
    bounded_len: usize,
    active: bool,
}

impl PesAssembler {
    fn begin(&mut self, chunk: &[u8], bounded_len: usize) {
        self.buf = chunk.to_vec();
        self.bounded_len = bounded_len;
        self.active = true;
    }

    fn complete(&self) -> bool {
        self.active && self.bounded_len != 0 && self.buf.len() >= 6 + self.bounded_len
    }
}

#[derive(Default)]
struct Demuxer {
    pmt_pid: Option<u16>,
    video_pid: Option<u16>,
    audio_pid: Option<u16>,
    sections: std::collections::HashMap<u16, Vec<u8>>,
    video_pes: PesAssembler,
    audio_pes: PesAssembler,
    video: Vec<Sample>,
    audio: Vec<Sample>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
    dims: (u16, u16),
    asc: Option<Vec<u8>>,
    audio_channels: u8,
    audio_rate: u32,
}

impl Demuxer {
    fn run(ts: &[u8]) -> Result<Demuxed, RemuxError> {
        // Resync to the first offset where the sync byte holds at the
        // packet stride (segments start at a packet boundary, but a
        // trimmed concatenation may not).
        let start = (0..TS_PACKET).find(|&i| {
            ts.len() >= i + 3 * TS_PACKET
                && ts[i] == 0x47
                && ts[i + TS_PACKET] == 0x47
                && ts[i + 2 * TS_PACKET] == 0x47
        });
        let Some(start) = start else {
            return Err(RemuxError::NotTs);
        };

        let mut d = Self {
            dims: (0, 0),
            ..Default::default()
        };

        let mut pos = start;
        while pos + TS_PACKET <= ts.len() {
            let pkt = &ts[pos..pos + TS_PACKET];
            pos += TS_PACKET;
            if pkt[0] != 0x47 || pkt[1] & 0x80 != 0 {
                continue; // bad sync / transport error
            }
            let pusi = pkt[1] & 0x40 != 0;
            let pid = (u16::from(pkt[1] & 0x1f) << 8) | u16::from(pkt[2]);
            let payload = match (pkt[3] >> 4) & 0b11 {
                0b00 | 0b10 => continue, // reserved / adaptation-only
                0b01 => &pkt[4..],
                _ => {
                    let af_len = usize::from(pkt[4]);
                    if 5 + af_len >= TS_PACKET {
                        continue;
                    }
                    &pkt[5 + af_len..]
                }
            };
            if payload.is_empty() {
                continue;
            }
            d.dispatch(payload, pusi, pid);
        }

        d.flush_video();
        d.flush_audio();
        d.into_demuxed()
    }

    fn dispatch(&mut self, payload: &[u8], pusi: bool, pid: u16) {
        if pid == 0 || Some(pid) == self.pmt_pid {
            self.psi(payload, pusi, pid);
        } else if Some(pid) == self.video_pid {
            self.video_payload(payload, pusi);
        } else if Some(pid) == self.audio_pid {
            self.audio_payload(payload, pusi);
        }
    }

    /// Reassembles and parses PAT/PMT sections.
    fn psi(&mut self, payload: &[u8], pusi: bool, pid: u16) {
        let entry = self.sections.entry(pid).or_default();
        if pusi {
            let pointer = usize::from(payload[0]);
            entry.clear();
            let begin = 1 + pointer;
            if begin <= payload.len() {
                entry.extend_from_slice(&payload[begin..]);
            }
        } else if !entry.is_empty() {
            entry.extend_from_slice(payload);
        } else {
            return;
        }

        for section in parse_sections(entry) {
            if pid == 0 {
                if self.pmt_pid.is_none() && section.len() >= 12 && section[0] == 0x00
                // PAT table id
                {
                    let mut i = 8;
                    while i + 4 <= section.len() {
                        let program = u16::from(section[i]) << 8 | u16::from(section[i + 1]);
                        if program != 0 {
                            self.pmt_pid = Some(
                                (u16::from(section[i + 2] & 0x1f) << 8) | u16::from(section[i + 3]),
                            );
                            break;
                        }
                        i += 4;
                    }
                }
            } else if section[0] == 0x02 && section.len() >= 12 {
                // PMT: pick the first video and first audio elementary stream.
                let program_info =
                    (usize::from(section[10] & 0x0f) << 8) | usize::from(section[11]);
                let mut i = 12 + program_info;
                while i + 5 <= section.len() {
                    let stream_type = section[i];
                    let es_pid =
                        (u16::from(section[i + 1] & 0x1f) << 8) | u16::from(section[i + 2]);
                    let es_len =
                        (usize::from(section[i + 3] & 0x0f) << 8) | usize::from(section[i + 4]);
                    match stream_type {
                        0x1b if self.video_pid.is_none() => self.video_pid = Some(es_pid),
                        0x0f if self.audio_pid.is_none() => self.audio_pid = Some(es_pid),
                        _ => {}
                    }
                    i += 5 + es_len;
                }
            }
        }
    }

    fn video_payload(&mut self, payload: &[u8], pusi: bool) {
        if pusi {
            // The previous PES ends where this one starts.
            self.flush_video();
            self.video_pes.begin(payload, pes_packet_length(payload));
        } else if self.video_pes.active {
            self.video_pes.buf.extend_from_slice(payload);
        }
        if self.video_pes.complete() {
            self.flush_video();
        }
    }

    fn audio_payload(&mut self, payload: &[u8], pusi: bool) {
        if pusi {
            self.flush_audio();
            self.audio_pes.begin(payload, pes_packet_length(payload));
        } else if self.audio_pes.active {
            self.audio_pes.buf.extend_from_slice(payload);
        }
        if self.audio_pes.complete() {
            self.flush_audio();
        }
    }

    fn flush_video(&mut self) {
        if let Some((pts, dts, es)) = finalize_pes(&mut self.video_pes)
            && let Some(sample) =
                video_sample_from_es(&es, pts, dts, &mut self.sps, &mut self.pps, &mut self.dims)
        {
            self.video.push(sample);
        }
    }

    fn flush_audio(&mut self) {
        if let Some((_pts, _dts, es)) = finalize_pes(&mut self.audio_pes) {
            audio_frames_from_es(
                &es,
                &mut self.audio,
                &mut self.asc,
                &mut self.audio_channels,
                &mut self.audio_rate,
            );
        }
    }

    fn into_demuxed(self) -> Result<Demuxed, RemuxError> {
        let Self {
            video,
            audio,
            sps,
            pps,
            dims,
            asc,
            audio_channels,
            audio_rate,
            ..
        } = self;
        if video.is_empty() && audio.is_empty() {
            return Err(RemuxError::NoStreams);
        }
        let (sps, pps) = match (sps, pps) {
            (Some(sps), Some(pps)) if !video.is_empty() => (sps, pps),
            _ => return Err(RemuxError::MissingParameterSets),
        };
        Ok(Demuxed {
            sps,
            pps,
            width: dims.0,
            height: dims.1,
            video,
            audio,
            asc: asc.unwrap_or_default(),
            audio_channels,
            audio_sample_rate: audio_rate,
        })
    }
}

fn pes_packet_length(payload: &[u8]) -> usize {
    if payload.len() >= 6 {
        (usize::from(payload[4]) << 8) | usize::from(payload[5])
    } else {
        0
    }
}

/// Splits an accumulated PSI buffer into complete sections (draining
/// the consumed bytes).
fn parse_sections(buf: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 3 <= buf.len() {
        let section_len = (usize::from(buf[i + 1] & 0x03) << 8) | usize::from(buf[i + 2]);
        let total = 3 + section_len;
        if i + total > buf.len() {
            break;
        }
        out.push(buf[i..i + total].to_vec());
        i += total;
    }
    buf.drain(..i);
    out
}

/// Finalizes one PES packet into (PTS, DTS, elementary stream bytes).
/// Incomplete or malformed packets are skipped (`None`) — a truncated
/// tail costs one frame, not the video.
fn finalize_pes(assembler: &mut PesAssembler) -> Option<(u64, u64, Vec<u8>)> {
    if !assembler.active {
        return None;
    }
    let mut buf = std::mem::take(&mut assembler.buf);
    let bounded = assembler.bounded_len;
    assembler.active = false;
    assembler.bounded_len = 0;

    // A bounded PES packet shares its final TS packet with zero stuffing;
    // trim the packet payload down to the declared PES length.
    if bounded != 0 && buf.len() > 6 + bounded {
        buf.truncate(6 + bounded);
    }

    if buf.len() < 9 || buf[0..3] != [0, 0, 1] {
        return None;
    }
    let header_len = usize::from(buf[8]);
    if buf.len() < 9 + header_len {
        return None;
    }
    let (pts, dts) = match (buf[7] >> 6) & 0b11 {
        0b10 => {
            let pts = parse_pts(&buf[9..14]);
            (pts, pts)
        }
        0b11 => (parse_pts(&buf[9..14]), parse_pts(&buf[14..19])),
        _ => (0, 0),
    };
    Some((pts, dts, buf[9 + header_len..].to_vec()))
}

fn parse_pts(b: &[u8]) -> u64 {
    (u64::from(b[0] >> 1) & 0x7) << 30
        | u64::from(b[1]) << 22
        | (u64::from(b[2] >> 1) & 0x7f) << 15
        | u64::from(b[3]) << 7
        | (u64::from(b[4] >> 1) & 0x7f)
}

/// Converts one video PES payload (an Annex-B access unit) into an AVCC
/// sample. Strips parameter sets into `sps`/`pps` (first sighting wins —
/// they seed the avcC record).
fn video_sample_from_es(
    es: &[u8],
    pts: u64,
    dts: u64,
    sps: &mut Option<Vec<u8>>,
    pps: &mut Option<Vec<u8>>,
    dims: &mut (u16, u16),
) -> Option<Sample> {
    let nals = split_annexb(es);
    let mut data: Vec<u8> = Vec::with_capacity(es.len());
    let mut idr = false;
    for nal in &nals {
        if nal.is_empty() {
            continue;
        }
        match nal[0] & 0x1f {
            7 => {
                if sps.is_none() {
                    if let Some(parsed) = sps_dimensions(nal) {
                        *dims = parsed;
                    }
                    *sps = Some(nal.to_vec());
                }
            }
            8 => {
                if pps.is_none() {
                    *pps = Some(nal.to_vec());
                }
            }
            9 => {} // access-unit delimiter: implied by sample boundaries
            5 => {
                idr = true;
                write_avcc_nal(&mut data, nal);
            }
            _ => write_avcc_nal(&mut data, nal),
        }
    }
    if data.is_empty() {
        return None;
    }
    Some(Sample {
        pts,
        dts,
        data,
        idr,
    })
}

fn write_avcc_nal(out: &mut Vec<u8>, nal: &[u8]) {
    out.extend_from_slice(&(nal.len() as u32).to_be_bytes());
    out.extend_from_slice(nal);
}

/// Splits an Annex-B elementary stream into NAL units (start-code
/// prefixes removed; both 00 00 01 and 00 00 00 01 forms handled).
fn split_annexb(es: &[u8]) -> Vec<&[u8]> {
    // Locate start codes, treating a 3-byte code as starting at the zero
    // before it when the 4-byte form is present.
    let mut starts: Vec<(usize, usize)> = Vec::new(); // (nal begin, payload start)
    let mut i = 0;
    while i + 2 < es.len() {
        if es[i] == 0 && es[i + 1] == 0 && es[i + 2] == 1 {
            let begin = if i > 0 && es[i - 1] == 0 { i - 1 } else { i };
            starts.push((begin, i + 3));
            i += 3;
        } else {
            i += 1;
        }
    }
    let mut nals = Vec::with_capacity(starts.len());
    for (n, &(_, payload_start)) in starts.iter().enumerate() {
        let end = starts
            .get(n + 1)
            .map(|&(begin, _)| begin)
            .unwrap_or(es.len());
        if payload_start < end {
            nals.push(&es[payload_start..end]);
        }
    }
    nals
}

/// Splits one audio PES payload (ADTS frames) into raw AAC samples,
/// deriving the AudioSpecificConfig from the first frame's header.
fn audio_frames_from_es(
    es: &[u8],
    audio: &mut Vec<Sample>,
    asc: &mut Option<Vec<u8>>,
    channels: &mut u8,
    rate: &mut u32,
) {
    let mut i = 0;
    while i + 7 <= es.len() {
        if es[i] != 0xff || (es[i + 1] & 0xf0) != 0xf0 {
            break;
        }
        let has_crc = es[i + 1] & 0x01 == 0;
        let header = 7 + usize::from(has_crc);
        let frame_len = (usize::from(es[i + 3] & 0x03) << 11)
            | (usize::from(es[i + 4]) << 3)
            | (usize::from(es[i + 5] >> 5) & 0x07);
        if frame_len < header || i + frame_len > es.len() {
            break;
        }
        if asc.is_none() {
            let profile = (es[i + 2] >> 6) & 0x03;
            let freq_idx = (es[i + 2] >> 2) & 0x0f;
            let chan_cfg = (es[i + 2] & 0x01) << 2 | ((es[i + 3] >> 6) & 0x03);
            *asc = Some(aac_asc(profile, freq_idx, chan_cfg));
            *channels = chan_cfg.max(1);
            *rate = aac_sample_rate(freq_idx);
        }
        audio.push(Sample {
            pts: 0,
            dts: 0,
            data: es[i + header..i + frame_len].to_vec(),
            idr: false,
        });
        i += frame_len;
    }
}

fn aac_asc(profile: u8, freq_idx: u8, chan_cfg: u8) -> Vec<u8> {
    let aot = u16::from(profile) + 1;
    let f = u16::from(freq_idx);
    let c = u16::from(chan_cfg);
    vec![
        ((aot << 3) | (f >> 1)) as u8,
        (((f & 1) << 7) | (c << 3)) as u8,
    ]
}

fn aac_sample_rate(freq_idx: u8) -> u32 {
    const RATES: [u32; 13] = [
        96_000, 88_200, 64_000, 48_000, 44_100, 32_000, 24_000, 22_050, 16_000, 12_000, 11_025,
        8_000, 7_350,
    ];
    RATES.get(usize::from(freq_idx)).copied().unwrap_or(48_000)
}

// ---------------------------------------------------------------------
// MP4 muxer (moov-first progressive)
// ---------------------------------------------------------------------

fn mux(d: &Demuxed) -> Result<Vec<u8>, RemuxError> {
    if d.video.is_empty() {
        return Err(RemuxError::NoVideo);
    }

    // ---- video track tables ----
    let video_sizes: Vec<u32> = d.video.iter().map(|s| s.data.len() as u32).collect();
    let mut stts: Vec<(u32, u32)> = Vec::new();
    for w in d.video.windows(2) {
        push_run(&mut stts, (w[1].dts - w[0].dts).min(u32::MAX as u64) as u32);
    }
    let last_delta = if d.video.len() > 1 {
        (d.video[d.video.len() - 1].dts - d.video[d.video.len() - 2].dts).min(u32::MAX as u64)
            as u32
    } else {
        3000
    };
    push_run(&mut stts, last_delta);
    let video_duration: u64 = video_duration_of(&stts);

    let has_ctts = d.video.iter().any(|s| s.pts > s.dts);
    let mut ctts: Vec<(u32, u32)> = Vec::new();
    if has_ctts {
        for s in &d.video {
            push_run(&mut ctts, (s.pts - s.dts).min(u32::MAX as u64) as u32);
        }
    }
    let idr_numbers: Vec<u32> = d
        .video
        .iter()
        .enumerate()
        .filter(|(_, s)| s.idr)
        .map(|(i, _)| (i + 1) as u32)
        .collect();

    // ---- audio track tables ----
    let has_audio = !d.audio.is_empty();
    let audio_sizes: Vec<u32> = d.audio.iter().map(|s| s.data.len() as u32).collect();
    let audio_stts: Vec<(u32, u32)> = if has_audio {
        vec![(d.audio.len() as u32, AAC_SAMPLES_PER_FRAME)]
    } else {
        Vec::new()
    };
    let audio_duration: u64 = u64::from(AAC_SAMPLES_PER_FRAME) * d.audio.len() as u64;

    let video_ms = video_duration * 1000 / u64::from(VIDEO_TIMESCALE);
    let audio_ms = audio_duration * 1000 / u64::from(d.audio_sample_rate.max(1));
    let duration_ms = video_ms.max(audio_ms);

    // ---- layout: ftyp | moov | mdat(video chunk, audio chunk) ----
    // Box sizes are independent of the chunk-offset VALUES: build moov
    // with placeholder offsets, measure, rebuild with real offsets.
    let mut ftyp: Vec<u8> = Vec::new();
    write_ftyp(&mut ftyp);
    let video_bytes: u64 = video_sizes.iter().map(|s| u64::from(*s)).sum();
    let audio_bytes: u64 = audio_sizes.iter().map(|s| u64::from(*s)).sum();
    if video_bytes + audio_bytes >= u64::from(u32::MAX) - 8 {
        // Would need 64-bit chunk offsets (co64); the size cap makes
        // this unreachable in practice.
        return Err(RemuxError::NoVideo);
    }

    let dummy = build_moov(
        d,
        &stts,
        &ctts,
        &idr_numbers,
        &video_sizes,
        &audio_stts,
        &audio_sizes,
        duration_ms,
        0,
        0,
    );
    let mdat_data_start = (ftyp.len() + dummy.len() + 8) as u64;
    let mut moov = build_moov(
        d,
        &stts,
        &ctts,
        &idr_numbers,
        &video_sizes,
        &audio_stts,
        &audio_sizes,
        duration_ms,
        mdat_data_start,
        mdat_data_start + video_bytes,
    );

    let mut out = ftyp;
    out.append(&mut moov);
    out.extend_from_slice(&((8 + video_bytes + audio_bytes) as u32).to_be_bytes());
    out.extend_from_slice(b"mdat");
    for s in &d.video {
        out.extend_from_slice(&s.data);
    }
    for s in &d.audio {
        out.extend_from_slice(&s.data);
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn build_moov(
    d: &Demuxed,
    stts: &[(u32, u32)],
    ctts: &[(u32, u32)],
    idr_numbers: &[u32],
    video_sizes: &[u32],
    audio_stts: &[(u32, u32)],
    audio_sizes: &[u32],
    duration_ms: u64,
    video_chunk_offset: u64,
    audio_chunk_offset: u64,
) -> Vec<u8> {
    let mut moov: Vec<u8> = Vec::new();
    let mut mvhd: Vec<u8> = Vec::new();
    mvhd.extend_from_slice(&0u32.to_be_bytes()); // version 0, flags 0
    mvhd.extend_from_slice(&0u32.to_be_bytes()); // creation
    mvhd.extend_from_slice(&0u32.to_be_bytes()); // modification
    mvhd.extend_from_slice(&1_000u32.to_be_bytes()); // timescale
    mvhd.extend_from_slice(&(duration_ms.min(u32::MAX as u64) as u32).to_be_bytes());
    mvhd.extend_from_slice(&0x0001_0000u32.to_be_bytes()); // rate
    mvhd.extend_from_slice(&0x0100u16.to_be_bytes()); // volume
    mvhd.extend_from_slice(&[0u8; 10]);
    mvhd.extend_from_slice(&[0u8; 36]);
    mvhd.extend_from_slice(&2u32.to_be_bytes()); // next_track_id
    write_box(&mut moov, b"mvhd", &mvhd);

    // ---- video trak ----
    let mut trak: Vec<u8> = Vec::new();
    let mut tkhd: Vec<u8> = Vec::new();
    tkhd.extend_from_slice(&3u32.to_be_bytes()); // version 0, flags: enabled+in_movie
    tkhd.extend_from_slice(&0u32.to_be_bytes());
    tkhd.extend_from_slice(&0u32.to_be_bytes());
    tkhd.extend_from_slice(&1u32.to_be_bytes()); // track_id
    tkhd.extend_from_slice(&0u32.to_be_bytes());
    tkhd.extend_from_slice(&(duration_ms.min(u32::MAX as u64) as u32).to_be_bytes());
    tkhd.extend_from_slice(&[0u8; 8]);
    tkhd.extend_from_slice(&0u16.to_be_bytes()); // layer
    tkhd.extend_from_slice(&0u16.to_be_bytes()); // alternate group
    tkhd.extend_from_slice(&0u16.to_be_bytes()); // volume
    tkhd.extend_from_slice(&0u16.to_be_bytes());
    tkhd.extend_from_slice(&IDENTITY_MATRIX);
    tkhd.extend_from_slice(&(u32::from(d.width.max(16)) << 16).to_be_bytes());
    tkhd.extend_from_slice(&(u32::from(d.height.max(16)) << 16).to_be_bytes());
    write_box(&mut trak, b"tkhd", &tkhd);

    let mut mdia: Vec<u8> = Vec::new();
    let mut mdhd: Vec<u8> = Vec::new();
    mdhd.extend_from_slice(&0u32.to_be_bytes());
    mdhd.extend_from_slice(&0u32.to_be_bytes());
    mdhd.extend_from_slice(&0u32.to_be_bytes());
    mdhd.extend_from_slice(&VIDEO_TIMESCALE.to_be_bytes());
    mdhd.extend_from_slice(&(video_duration_of(stts).min(u32::MAX as u64) as u32).to_be_bytes());
    mdhd.extend_from_slice(&0x55c4u16.to_be_bytes()); // und
    mdhd.extend_from_slice(&0u16.to_be_bytes());
    write_box(&mut mdia, b"mdhd", &mdhd);
    write_box(&mut mdia, b"hdlr", &hdlr(b"vide", b"VideoHandler"));

    let mut minf: Vec<u8> = Vec::new();
    write_box(&mut minf, b"vmhd", &[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]);
    write_box(&mut minf, b"dinf", &dinf());
    let mut stbl: Vec<u8> = Vec::new();
    write_box(&mut stbl, b"stsd", &video_stsd(d));
    write_box(&mut stbl, b"stts", &stts_box(stts));
    if !ctts.is_empty() {
        write_box(&mut stbl, b"ctts", &ctts_box(ctts));
    }
    if idr_numbers.len() != video_sizes.len() {
        write_box(&mut stbl, b"stss", &stss_box(idr_numbers));
    }
    write_box(&mut stbl, b"stsc", &stsc_single_chunk(video_sizes.len()));
    write_box(&mut stbl, b"stsz", &stsz_box(video_sizes));
    write_box(&mut stbl, b"stco", &stco_box(video_chunk_offset));
    write_box(&mut minf, b"stbl", &stbl);
    write_box(&mut mdia, b"minf", &minf);
    write_box(&mut trak, b"mdia", &mdia);
    write_box(&mut moov, b"trak", &trak);

    // ---- audio trak ----
    if !audio_sizes.is_empty() {
        let mut trak: Vec<u8> = Vec::new();
        let mut tkhd: Vec<u8> = Vec::new();
        tkhd.extend_from_slice(&3u32.to_be_bytes());
        tkhd.extend_from_slice(&0u32.to_be_bytes());
        tkhd.extend_from_slice(&0u32.to_be_bytes());
        tkhd.extend_from_slice(&2u32.to_be_bytes()); // track_id
        tkhd.extend_from_slice(&0u32.to_be_bytes());
        tkhd.extend_from_slice(&(duration_ms.min(u32::MAX as u64) as u32).to_be_bytes());
        tkhd.extend_from_slice(&[0u8; 8]);
        tkhd.extend_from_slice(&0u16.to_be_bytes());
        tkhd.extend_from_slice(&0u16.to_be_bytes());
        tkhd.extend_from_slice(&0x0100u16.to_be_bytes()); // volume
        tkhd.extend_from_slice(&0u16.to_be_bytes());
        tkhd.extend_from_slice(&IDENTITY_MATRIX);
        tkhd.extend_from_slice(&0u32.to_be_bytes());
        tkhd.extend_from_slice(&0u32.to_be_bytes());
        write_box(&mut trak, b"tkhd", &tkhd);

        let mut mdia: Vec<u8> = Vec::new();
        let mut mdhd: Vec<u8> = Vec::new();
        mdhd.extend_from_slice(&0u32.to_be_bytes());
        mdhd.extend_from_slice(&0u32.to_be_bytes());
        mdhd.extend_from_slice(&0u32.to_be_bytes());
        mdhd.extend_from_slice(&d.audio_sample_rate.max(1).to_be_bytes());
        let samples = u64::from(AAC_SAMPLES_PER_FRAME) * audio_sizes.len() as u64;
        mdhd.extend_from_slice(&(samples.min(u32::MAX as u64) as u32).to_be_bytes());
        mdhd.extend_from_slice(&0x55c4u16.to_be_bytes());
        mdhd.extend_from_slice(&0u16.to_be_bytes());
        write_box(&mut mdia, b"mdhd", &mdhd);
        write_box(&mut mdia, b"hdlr", &hdlr(b"soun", b"SoundHandler"));

        let mut minf: Vec<u8> = Vec::new();
        write_box(&mut minf, b"smhd", &[0, 0, 0, 1, 0, 0, 0, 0]);
        write_box(&mut minf, b"dinf", &dinf());
        let mut stbl: Vec<u8> = Vec::new();
        write_box(&mut stbl, b"stsd", &audio_stsd(d));
        write_box(&mut stbl, b"stts", &stts_box(audio_stts));
        write_box(&mut stbl, b"stsc", &stsc_single_chunk(audio_sizes.len()));
        write_box(&mut stbl, b"stsz", &stsz_box(audio_sizes));
        write_box(&mut stbl, b"stco", &stco_box(audio_chunk_offset));
        write_box(&mut minf, b"stbl", &stbl);
        write_box(&mut mdia, b"minf", &minf);
        write_box(&mut trak, b"mdia", &mdia);
        write_box(&mut moov, b"trak", &trak);
    }

    let mut wrapped = Vec::with_capacity(moov.len() + 8);
    write_box(&mut wrapped, b"moov", &moov);
    wrapped
}

/// Total track duration (in the track timescale) from an stts run-length
/// table: the sum of every sample's decode delta.
fn video_duration_of(stts: &[(u32, u32)]) -> u64 {
    stts.iter()
        .map(|(count, delta)| u64::from(*count) * u64::from(*delta))
        .sum()
}

/// Unity transform matrix, as ISMACROP expects: three 16.16 row values
/// (1, 0, 0 / 0, 1, 0 / 0, 0, 0.25).
const IDENTITY_MATRIX: [u8; 36] = {
    let mut m = [0u8; 36];
    m[1] = 1; // 0x00010000
    m[16 + 1] = 1;
    m[32 + 2] = 0x40; // 0x40000000
    m
};

fn push_run(entries: &mut Vec<(u32, u32)>, delta: u32) {
    match entries.last_mut() {
        Some((count, last)) if *last == delta => *count += 1,
        _ => entries.push((1, delta)),
    }
}

fn write_box(out: &mut Vec<u8>, kind: &[u8; 4], payload: &[u8]) {
    out.extend_from_slice(&((payload.len() as u32) + 8).to_be_bytes());
    out.extend_from_slice(kind);
    out.extend_from_slice(payload);
}

fn write_ftyp(out: &mut Vec<u8>) {
    let payload = [
        b"isom".as_slice(),
        &512u32.to_be_bytes(),
        b"isom".as_slice(),
        b"iso2".as_slice(),
        b"avc1".as_slice(),
        b"mp41".as_slice(),
    ]
    .concat();
    write_box(out, b"ftyp", &payload);
}

fn hdlr(handler: &[u8; 4], name: &[u8]) -> Vec<u8> {
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(handler);
    b.extend_from_slice(&[0u8; 12]);
    b.extend_from_slice(name);
    b.push(0);
    b
}

fn dinf() -> Vec<u8> {
    let mut dref: Vec<u8> = Vec::new();
    dref.extend_from_slice(&0u32.to_be_bytes());
    dref.extend_from_slice(&1u32.to_be_bytes());
    write_box(&mut dref, b"url ", &[0, 0, 0, 1]); // self-contained
    let mut d: Vec<u8> = Vec::new();
    write_box(&mut d, b"dref", &dref);
    d
}

fn stts_box(entries: &[(u32, u32)]) -> Vec<u8> {
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for (count, delta) in entries {
        b.extend_from_slice(&count.to_be_bytes());
        b.extend_from_slice(&delta.to_be_bytes());
    }
    b
}

fn ctts_box(entries: &[(u32, u32)]) -> Vec<u8> {
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for (count, offset) in entries {
        b.extend_from_slice(&count.to_be_bytes());
        b.extend_from_slice(&offset.to_be_bytes());
    }
    b
}

fn stss_box(samples: &[u32]) -> Vec<u8> {
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&(samples.len() as u32).to_be_bytes());
    for s in samples {
        b.extend_from_slice(&s.to_be_bytes());
    }
    b
}

fn stsc_single_chunk(sample_count: usize) -> Vec<u8> {
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes()); // entry count
    b.extend_from_slice(&1u32.to_be_bytes()); // first chunk
    b.extend_from_slice(&(sample_count.min(u32::MAX as usize) as u32).to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes()); // sample description index
    b
}

fn stsz_box(sizes: &[u32]) -> Vec<u8> {
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&0u32.to_be_bytes()); // non-uniform sizes
    b.extend_from_slice(&(sizes.len() as u32).to_be_bytes());
    for s in sizes {
        b.extend_from_slice(&s.to_be_bytes());
    }
    b
}

fn stco_box(offset: u64) -> Vec<u8> {
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&(offset.min(u32::MAX as u64) as u32).to_be_bytes());
    b
}

fn video_stsd(d: &Demuxed) -> Vec<u8> {
    let mut avc1: Vec<u8> = Vec::new();
    avc1.extend_from_slice(&[0u8; 6]);
    avc1.extend_from_slice(&1u16.to_be_bytes()); // data reference index
    avc1.extend_from_slice(&0u16.to_be_bytes()); // pre_defined
    avc1.extend_from_slice(&0u16.to_be_bytes()); // reserved
    avc1.extend_from_slice(&[0u8; 12]); // pre_defined + reserved
    avc1.extend_from_slice(&d.width.max(16).to_be_bytes());
    avc1.extend_from_slice(&d.height.max(16).to_be_bytes());
    avc1.extend_from_slice(&0x0048_0000u32.to_be_bytes()); // 72 dpi
    avc1.extend_from_slice(&0x0048_0000u32.to_be_bytes());
    avc1.extend_from_slice(&0u32.to_be_bytes());
    avc1.extend_from_slice(&1u16.to_be_bytes()); // frame count
    avc1.extend_from_slice(&[0u8; 32]); // compressor name
    avc1.extend_from_slice(&0x0018u16.to_be_bytes()); // depth
    avc1.extend_from_slice(&0xffffu16.to_be_bytes());
    write_box(&mut avc1, b"avcC", &avcc_record(&d.sps, &d.pps));
    let mut stsd: Vec<u8> = Vec::new();
    stsd.extend_from_slice(&0u32.to_be_bytes());
    stsd.extend_from_slice(&1u32.to_be_bytes());
    write_box(&mut stsd, b"avc1", &avc1);
    stsd
}

fn audio_stsd(d: &Demuxed) -> Vec<u8> {
    let mut mp4a: Vec<u8> = Vec::new();
    mp4a.extend_from_slice(&[0u8; 6]);
    mp4a.extend_from_slice(&1u16.to_be_bytes());
    mp4a.extend_from_slice(&[0u8; 8]); // reserved (2×u32)
    mp4a.extend_from_slice(&u16::from(d.audio_channels.max(1)).to_be_bytes());
    mp4a.extend_from_slice(&16u16.to_be_bytes()); // sample size
    mp4a.extend_from_slice(&0u16.to_be_bytes()); // pre_defined
    mp4a.extend_from_slice(&0u16.to_be_bytes()); // reserved
    mp4a.extend_from_slice(&(d.audio_sample_rate.max(1) << 16).to_be_bytes()); // 16.16
    let esds = esds_box(&d.asc, d.audio_sample_rate);
    write_box(&mut mp4a, b"esds", &esds);
    let mut stsd: Vec<u8> = Vec::new();
    stsd.extend_from_slice(&0u32.to_be_bytes());
    stsd.extend_from_slice(&1u32.to_be_bytes());
    write_box(&mut stsd, b"mp4a", &mp4a);
    stsd
}

/// Full-box wrapper around an ES_Descriptor for AAC.
fn esds_box(asc: &[u8], sample_rate: u32) -> Vec<u8> {
    let dsi = tag_payload(0x05, asc);
    let mut dcd: Vec<u8> = Vec::new();
    dcd.push(0x40); // object type: AAC
    dcd.push(0x15); // audio stream type, reserved bit
    dcd.extend_from_slice(&[0, 0, 0]); // buffer size db
    dcd.extend_from_slice(&0u32.to_be_bytes()); // max bitrate
    dcd.extend_from_slice(
        &((u64::from(sample_rate) / 4).min(u32::MAX as u64) as u32).to_be_bytes(),
    ); // avg bitrate (rough)
    dcd.extend_from_slice(&dsi);
    let dcd = tag_payload(0x04, &dcd);
    let sl = tag_payload(0x06, &[0x02]);
    let mut es: Vec<u8> = Vec::new();
    es.extend_from_slice(&1u16.to_be_bytes()); // ES_ID
    es.push(0); // flags
    es.extend_from_slice(&dcd);
    es.extend_from_slice(&sl);
    let es = tag_payload(0x03, &es);
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&0u32.to_be_bytes()); // FullBox version/flags
    b.extend_from_slice(&es);
    b
}

/// Expandable-descriptor tag: tag byte + varint length + payload.
fn tag_payload(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let mut lengths: Vec<u8> = vec![(payload.len() as u8) & 0x7f];
    let mut len = payload.len() >> 7;
    while len > 0 {
        lengths.push((len as u8 & 0x7f) | 0x80);
        len >>= 7;
    }
    for l in lengths.iter().rev() {
        out.push(*l);
    }
    out.extend_from_slice(payload);
    out
}

/// AVCDecoderConfigurationRecord from the stream's SPS + PPS.
fn avcc_record(sps: &[u8], pps: &[u8]) -> Vec<u8> {
    let mut b: Vec<u8> = vec![
        1,        // configurationVersion
        sps[1],   // profile
        sps[2],   // compat
        sps[3],   // level
        0xfc | 3, // reserved + NAL length size: 3+1 = 4 bytes
        0xe0 | 1, // reserved + 1 SPS
    ];
    b.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    b.extend_from_slice(sps);
    b.push(1); // 1 PPS
    b.extend_from_slice(&(pps.len() as u16).to_be_bytes());
    b.extend_from_slice(pps);
    b
}

// ---------------------------------------------------------------------
// SPS parsing (display dimensions for the sample entry)
// ---------------------------------------------------------------------

/// Minimal SPS parser: coded width/height including frame cropping.
/// Returns `None` for anything this parser doesn't handle (the muxer
/// then falls back to placeholder dimensions; decoding is unaffected —
/// the real geometry lives in avcC's SPS).
fn sps_dimensions(sps: &[u8]) -> Option<(u16, u16)> {
    let rbsp = unescape_rbsp(&sps[1..]); // skip the NAL header byte
    let mut bits = BitReader::new(&rbsp);

    let profile_idc = bits.read(8);
    let _compat = bits.read(8);
    let _level = bits.read(8);
    let _sps_id = bits.read_ue();

    if matches!(
        profile_idc,
        100 | 110 | 122 | 244 | 44 | 83 | 86 | 118 | 128 | 138 | 139 | 134 | 135
    ) {
        let chroma_format_idc = bits.read_ue();
        if chroma_format_idc == 3 {
            bits.read(1); // separate_colour_plane_flag
        }
        bits.read_ue(); // bit_depth_luma_minus8
        bits.read_ue(); // bit_depth_chroma_minus8
        bits.read(1); // qpprime_y_zero
        if bits.read(1) == 1 {
            // seq_scaling_matrix_present
            let count = if chroma_format_idc == 3 { 12 } else { 8 };
            for _ in 0..count {
                if bits.read(1) == 1 {
                    skip_scaling_list(&mut bits);
                }
            }
        }
    }

    bits.read_ue(); // log2_max_frame_num
    match bits.read_ue() {
        0 => {
            bits.read_ue(); // log2_max_pic_order_cnt_lsb
        }
        1 => {
            bits.read(1); // delta_pic_order_always_zero
            bits.read_se(); // offset_for_non_ref_pic
            bits.read_se(); // offset_for_top_to_bottom_field
            let n = bits.read_ue();
            for _ in 0..n {
                bits.read_se();
            }
        }
        2 => {}
        _ => return None,
    }
    bits.read_ue(); // max_num_ref_frames
    bits.read(1); // gaps_in_frame_num_allowed
    let pic_width_mbs = bits.read_ue() + 1;
    let pic_height_map_units = bits.read_ue() + 1;
    let frame_mbs_only = bits.read(1);
    let height_units = if frame_mbs_only == 1 {
        pic_height_map_units
    } else {
        bits.read(1); // mb_adaptive_frame_field
        pic_height_map_units * 2
    };
    bits.read(1); // direct_8x8_inference
    let mut width = pic_width_mbs * 16;
    let mut height = height_units * 16;
    if bits.read(1) == 1 {
        // frame_cropping; crop units for the common 4:2:0 case.
        let crop_left = u64::from(bits.read_ue());
        let crop_right = u64::from(bits.read_ue());
        let crop_top = u64::from(bits.read_ue());
        let crop_bottom = u64::from(bits.read_ue());
        width = (u64::from(width)).saturating_sub((crop_left + crop_right) * 2) as u32;
        height = (u64::from(height)).saturating_sub((crop_top + crop_bottom) * 2) as u32;
    }
    Some((u16::try_from(width).ok()?, u16::try_from(height).ok()?))
}

fn skip_scaling_list(bits: &mut BitReader) {
    let size = bits.read_ue();
    let count = if size < 6 { 16 } else { 64 };
    let mut last = 8i64;
    let mut next = 8i64;
    for _ in 0..count {
        if next != 0 {
            next = (last + bits.read_se()) & 0xff_ff_ff_ff;
            last = if next == 0 { last } else { next };
        }
    }
}

/// Strips H.264 emulation-prevention bytes (00 00 03).
fn unescape_rbsp(nal: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(nal.len());
    let mut zeros = 0;
    for &b in nal {
        if zeros >= 2 && b == 3 {
            zeros = 0;
            continue;
        }
        zeros = if b == 0 { zeros + 1 } else { 0 };
        out.push(b);
    }
    out
}

struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read(&mut self, n: u32) -> u32 {
        let mut v = 0u32;
        for _ in 0..n {
            let byte = self.data.get(self.pos / 8).copied().unwrap_or(0);
            v = (v << 1) | u32::from((byte >> (7 - (self.pos % 8))) & 1);
            self.pos += 1;
        }
        v
    }

    fn read_ue(&mut self) -> u32 {
        let mut zeros = 0;
        while self.read(1) == 0 && zeros < 31 {
            zeros += 1;
        }
        if zeros == 0 {
            return 0;
        }
        (1 << zeros) - 1 + self.read(zeros)
    }

    fn read_se(&mut self) -> i64 {
        let k = self.read_ue() as i64;
        if k % 2 == 1 { (k + 1) / 2 } else { -(k / 2) }
    }
}

/// A tiny but structurally-real MPEG-TS stream (PAT, PMT, a few H.264
/// access units, one AAC frame) shared with other modules' tests: the
/// HLS download path remuxes exactly this shape of stream.
#[cfg(test)]
pub(crate) fn synthetic_test_ts() -> Vec<u8> {
    fn ts_packet(pusi: bool, pid: u16, payload: &[u8]) -> Vec<u8> {
        let mut pkt = vec![0u8; TS_PACKET];
        pkt[0] = 0x47;
        pkt[1] = (if pusi { 0x40 } else { 0 }) | ((pid >> 8) as u8 & 0x1f);
        pkt[2] = pid as u8;
        pkt[3] = 0x10;
        let space = TS_PACKET - 4;
        let n = payload.len().min(space);
        pkt[4..4 + n].copy_from_slice(&payload[..n]);
        pkt
    }

    fn pes_packet(pts: u64, stream_id: u8, es: &[u8]) -> Vec<u8> {
        let mut pes: Vec<u8> = vec![0, 0, 1, stream_id];
        let header = [0x80u8, 0x80, 5];
        let v = pts & 0x1_ffff_ffff;
        let pts_bytes = vec![
            0x21 | ((((v >> 30) & 0x7) as u8) << 1),
            ((v >> 22) & 0xff) as u8,
            0x01 | ((((v >> 15) & 0x7f) as u8) << 1),
            ((v >> 7) & 0xff) as u8,
            0x01 | (((v & 0x7f) as u8) << 1),
        ];
        let len = header.len() + pts_bytes.len() + es.len();
        pes.extend_from_slice(&(len as u16).to_be_bytes());
        pes.extend_from_slice(&header);
        pes.extend_from_slice(&pts_bytes);
        pes.extend_from_slice(es);
        pes
    }

    let mut ts: Vec<u8> = Vec::new();
    let mut pat: Vec<u8> = vec![0x00, 0xb0, 0x0d, 0x00, 0x01, 0xc1, 0x00, 0x00];
    pat.extend_from_slice(&[0x00, 0x01, 0x10, 0x00]);
    pat.extend_from_slice(&[0, 0, 0, 0]);
    ts.extend(ts_packet(true, 0, &[&[0x00][..], &pat].concat()));

    let mut pmt: Vec<u8> = vec![
        0x02, 0xb0, 0x17, 0x00, 0x01, 0xc1, 0x00, 0x00, 0xe1, 0x00, 0xf0, 0x00,
    ];
    pmt.extend_from_slice(&[0x1b, 0xe1, 0x01, 0xf0, 0x00]);
    pmt.extend_from_slice(&[0x0f, 0xe1, 0x02, 0xf0, 0x00]);
    pmt.extend_from_slice(&[0, 0, 0, 0]);
    ts.extend(ts_packet(true, 0x1000, &[&[0x00][..], &pmt].concat()));

    for (i, au) in TEST_VIDEO_AUS.iter().enumerate() {
        let pes = pes_packet((i as u64 + 1) * 3000, 0xe0, au);
        for (n, chunk) in pes.chunks(TS_PACKET - 4).enumerate() {
            ts.extend(ts_packet(n == 0, 0x101, chunk));
        }
    }

    let adts: Vec<u8> = vec![0xff, 0xf1, 0x4c, 0x80, 0x01, 0x3f, 0xfc, 0xde, 0xad];
    let pes = pes_packet(3000, 0xc0, &adts);
    for (n, chunk) in pes.chunks(TS_PACKET - 4).enumerate() {
        ts.extend(ts_packet(n == 0, 0x102, chunk));
    }
    ts
}

#[cfg(test)]
const TEST_VIDEO_AUS: &[&[u8]] = &[
    // Access unit 0: SPS + PPS + IDR slice.
    &[
        0, 0, 0, 1, 0x67, 0x4d, 0x40, 0x1f, 0xe8, 0x80, 0x80, 0x40, 0x00, 0x00, 0x03, 0x00, 0x80,
        0x00, 0x00, 0x1e, 0x60, 0x0c, 0x20, 0, 0, 0, 1, 0x68, 0xeb, 0xec, 0xb2, 0x2c, 0, 0, 0, 1,
        0x65, 0x11,
    ],
    // Non-IDR slices.
    &[0, 0, 0, 1, 0x41, 0x22],
    &[0, 0, 0, 1, 0x41, 0x22],
];

#[cfg(test)]
mod tests;
