use super::*;

#[test]
fn ts_detection_requires_sync_at_stride() {
    assert!(!looks_like_mpeg_ts(b"#EXTM3U\nsegment\n"));
    assert!(!looks_like_mpeg_ts(&[0x47; 100]));
    let mut ts = vec![0x47u8; 3 * TS_PACKET];
    assert!(looks_like_mpeg_ts(&ts));
    ts[TS_PACKET] = 0;
    assert!(!looks_like_mpeg_ts(&ts));
}

#[test]
fn annexb_split_handles_both_start_code_forms() {
    let es = [0, 0, 1, 0x65, 1, 2, 0, 0, 0, 1, 0x41, 3, 0, 0, 1, 0x41, 4];
    let nals = split_annexb(&es);
    assert_eq!(
        nals,
        vec![&[0x65, 1, 2][..], &[0x41, 3][..], &[0x41, 4][..]]
    );
}

#[test]
fn pts_decoding_matches_spec_layout() {
    let v: u64 = (5 << 30) | (0x1234 << 15) | 0x567;
    let b0 = 0x21 | (((v >> 30) as u8) << 1);
    let b1 = (v >> 22) as u8;
    let b2 = 0x01 | ((((v >> 15) as u8) & 0x7f) << 1);
    let b3 = (v >> 7) as u8;
    let b4 = 0x01 | (((v as u8) & 0x7f) << 1);
    assert_eq!(parse_pts(&[b0, b1, b2, b3, b4]), v);
}

#[test]
fn adts_frames_split_into_raw_aac_with_asc() {
    let frame = |payload: &[u8]| -> Vec<u8> {
        let mut f: Vec<u8> = vec![0xff, 0xf1, 0x4c, 0x80, 0, 0, 0xfc];
        // profile=01 (LC), freq_idx=0011 (48k), chan bits start "10"
        let len = 7 + payload.len();
        f[3] = 0x80 | (((len >> 11) as u8) & 0x03) << 2;
        f[4] = ((len >> 3) & 0xff) as u8;
        f[5] = (((len & 0x07) << 5) as u8) | 0x1f;
        f.extend_from_slice(payload);
        f
    };
    let mut es = frame(b"aaaaaaaa");
    es.extend(frame(b"bbbbbbbbbb"));

    let mut audio = Vec::new();
    let mut asc = None;
    let mut channels = 0;
    let mut rate = 0;
    audio_frames_from_es(&es, &mut audio, &mut asc, &mut channels, &mut rate);
    assert_eq!(audio.len(), 2);
    assert_eq!(audio[0].data, b"aaaaaaaa");
    assert_eq!(audio[1].data, b"bbbbbbbbbb");
    assert_eq!(rate, 48_000);
    assert_eq!(channels, 2);
    assert_eq!(asc.unwrap(), aac_asc(1, 3, 2));
}

#[test]
fn sps_dimensions_reads_a_constructed_main_profile_sps() {
    // Hand-built SPS for 64x48, baseline-ish fields only.
    let mut bits: Vec<bool> = Vec::new();
    let push = |bits: &mut Vec<bool>, v: u32, n: u32| {
        for i in (0..n).rev() {
            bits.push((v >> i) & 1 == 1);
        }
    };
    push(&mut bits, 66, 8); // baseline profile
    push(&mut bits, 0, 8); // constraint flags
    push(&mut bits, 30, 8); // level
    write_ue(&mut bits, 0); // sps id
    write_ue(&mut bits, 4); // log2_max_frame_num
    write_ue(&mut bits, 0); // pic_order_cnt_type
    write_ue(&mut bits, 4); // log2_max_pic_order_cnt
    write_ue(&mut bits, 1); // max_num_ref_frames
    bits.push(false); // gaps
    write_ue(&mut bits, 3); // width in MBs - 1 → 64
    write_ue(&mut bits, 2); // height map units - 1 → 48
    bits.push(true); // frame_mbs_only
    bits.push(true); // direct_8x8
    bits.push(false); // no crop
    bits.push(false); // no vui
    bits.push(true); // rbsp stop bit

    let mut nal = vec![0x67];
    nal.extend(bits_to_bytes(&bits));
    assert_eq!(sps_dimensions(&nal), Some((64, 48)));
}

fn write_ue(bits: &mut Vec<bool>, v: u32) {
    let k = v + 1;
    let n = 31 - k.leading_zeros(); // bit length of k, minus 1
    for _ in 0..n {
        bits.push(false);
    }
    for i in (0..=n).rev() {
        bits.push((k >> i) & 1 == 1);
    }
}

fn bits_to_bytes(bits: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; bits.len().div_ceil(8)];
    for (i, &b) in bits.iter().enumerate() {
        if b {
            out[i / 8] |= 1 << (7 - (i % 8));
        }
    }
    out
}

#[test]
fn synthetic_ts_stream_remuxes_into_valid_boxes() {
    let ts = synthetic_test_ts();
    let mp4 = remux_ts_to_mp4(&ts).expect("remuxes");

    let mut i = 0;
    let mut kinds = Vec::new();
    let mut mdat_payload = 0usize;
    while i + 8 <= mp4.len() {
        let size = u32::from_be_bytes([mp4[i], mp4[i + 1], mp4[i + 2], mp4[i + 3]]) as usize;
        let kind = &mp4[i + 4..i + 8];
        kinds.push(std::str::from_utf8(kind).unwrap().to_string());
        assert!(size >= 8 && i + size <= mp4.len(), "box {kind:?} malformed");
        if kind == b"mdat" {
            mdat_payload = size - 8;
        }
        i += size;
    }
    assert_eq!(kinds, vec!["ftyp", "moov", "mdat"]);

    // mdat must hold every sample's AVCC payload (4-byte length
    // prefix + NAL data) exactly once, plus the raw AAC frame.
    let video_bytes: usize = TEST_VIDEO_AUS
        .iter()
        .map(|au| {
            split_annexb(au)
                .iter()
                .filter(|nal| !matches!(nal[0] & 0x1f, 7..=9))
                .map(|nal| 4 + nal.len())
                .sum::<usize>()
        })
        .sum();
    assert_eq!(mdat_payload, video_bytes + 2); // audio frames go in raw (no length prefix)
}

#[test]
fn moov_chunk_offsets_point_into_mdat() {
    let ts = synthetic_test_ts();
    let mp4 = remux_ts_to_mp4(&ts).expect("remuxes");
    let mdat_start = find_box(&mp4, b"mdat").expect("mdat") as u64;
    let video_offset = track_chunk_offset(&mp4, b"avc1").expect("video stco");
    let audio_offset = track_chunk_offset(&mp4, b"mp4a").expect("audio stco");
    assert_eq!(video_offset, mdat_start + 8);
    assert!(audio_offset > video_offset);
    assert!(audio_offset < mp4.len() as u64);
}

// ---- synthetic TS construction helpers ----

fn find_box(mp4: &[u8], kind: &[u8; 4]) -> Option<usize> {
    let mut i = 0;
    while i + 8 <= mp4.len() {
        let size = u32::from_be_bytes([mp4[i], mp4[i + 1], mp4[i + 2], mp4[i + 3]]) as usize;
        if &mp4[i + 4..i + 8] == kind {
            return Some(i);
        }
        i += size.max(8);
    }
    None
}

/// Finds the first chunk offset of the track whose stsd carries the
/// given sample entry (`avc1`/`mp4a`), searching each trak separately.
fn track_chunk_offset(mp4: &[u8], sample_entry: &[u8; 4]) -> Option<u64> {
    fn walk_trak(mp4: &[u8], sample_entry: &[u8; 4]) -> Option<u64> {
        let mut seen_entry = false;
        walk(mp4, sample_entry, &mut seen_entry)
    }
    fn walk(mp4: &[u8], sample_entry: &[u8; 4], seen_entry: &mut bool) -> Option<u64> {
        let mut i = 0;
        while i + 8 <= mp4.len() {
            let size = u32::from_be_bytes([mp4[i], mp4[i + 1], mp4[i + 2], mp4[i + 3]]) as usize;
            let kind = &mp4[i + 4..i + 8];
            let payload = &mp4[i + 8..i + size];
            if kind == b"trak" {
                if let Some(v) = walk_trak(payload, sample_entry) {
                    return Some(v);
                }
            } else if kind == sample_entry {
                *seen_entry = true;
            } else if kind == b"stco" && *seen_entry {
                let entries = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                assert_eq!(entries, 1);
                return Some(
                    u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]) as u64,
                );
            } else if kind == b"stsd" {
                // FullBox: version/flags AND the entry-count word
                // precede the sample-entry boxes.
                if let Some(v) = walk(&payload[8..], sample_entry, seen_entry) {
                    return Some(v);
                }
            } else if matches!(kind, b"moov" | b"mdia" | b"minf" | b"stbl")
                && let Some(v) = walk(payload, sample_entry, seen_entry)
            {
                return Some(v);
            }
            i += size.max(8);
        }
        None
    }
    walk(mp4, sample_entry, &mut false)
}
