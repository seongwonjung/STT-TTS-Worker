# Split Up & Chunk Work 파이프라인 프로세스

이 문서는 백엔드에서 `task == "split_up"` 메시지를 SQS에 전송했을 때 워커가 처리하는 전체 프로세스를 설명합니다.

## 목차

1. [전체 프로세스 개요](#전체-프로세스-개요)
2. [1단계: 백엔드에서 SQS 메시지 전송](#1단계-백엔드에서-sqs-메시지-전송)
3. [2단계: 워커가 SQS 메시지 수신 및 라우팅](#2단계-워커가-sqs-메시지-수신-및-라우팅)
4. [3단계: split_up 파이프라인 실행](#3단계-split_up-파이프라인-실행)
5. [4단계: chunk_work 파이프라인 실행](#4단계-chunk_work-파이프라인-실행)
6. [S3 다운로드/업로드 요약](#s3-다운로드업로드-요약)
7. [Voice Replacement 프로세스](#voice-replacement-프로세스)

---

## 전체 프로세스 개요

```
백엔드 → SQS (split_up 메시지) 
    ↓
워커 수신 → split_up 파이프라인 실행
    ├─ S3에서 원본 영상 다운로드
    ├─ ASR (STT) 수행
    ├─ Voice Replacement 준비 (선택적)
    ├─ 청크 생성
    ├─ Manifest.json 생성 및 업로드
    └─ 각 청크를 chunk_work 큐에 전송
         ↓
    각 청크별 chunk_work 파이프라인 실행
    ├─ Manifest.json 다운로드
    ├─ Compact Transcript 다운로드
    ├─ Speaker References 다운로드
    ├─ 청크 범위 세그먼트 필터링
    ├─ 번역 수행
    ├─ Voice Replacement 적용 (선택적)
    ├─ TTS 수행
    ├─ Sync 수행
    └─ 완료 체크 및 Mux 트리거
```

---

## 1단계: 백엔드에서 SQS 메시지 전송

백엔드는 `backend/app/api/jobs/service.py`의 `_build_job_message` 함수를 통해 메시지를 구성합니다.

### 메시지 구성

```python
# backend/app/api/jobs/service.py (Lines 67-97)
def _build_job_message(job: JobRead) -> dict[str, Any]:
    task = job.task or "split_up"
    message: dict[str, Any] = {
        "task": task,
        "job_id": job.job_id,
        "project_id": job.project_id,
        "callback_url": str(job.callback_url),
    }
    if job.input_key:
        message["input_key"] = job.input_key
    if job.source_lang:
        message["source_lang"] = job.source_lang
    if job.target_lang:
        message["target_lang"] = job.target_lang
    if job.is_replace_voice_samples:
        message["is_replace_voice_samples"] = job.is_replace_voice_samples
    
    payload = job.task_payload or {}
    if task == "segment_tts":
        if payload:
            message.update(payload)
    elif payload:
        message.update(payload)
    
    return message
```

### 메시지에 포함되는 주요 필드

- `task`: "split_up"
- `job_id`: 작업 ID
- `project_id`: 프로젝트 ID
- `callback_url`: 콜백 URL
- `input_key`: S3에 업로드된 원본 영상 키
- `source_lang`: 원본 언어
- `target_lang`: 타겟 언어
- `is_replace_voice_samples`: Voice replacement 활성화 여부
- 기타 `task_payload`에 포함된 설정들

---

## 2단계: 워커가 SQS 메시지 수신 및 라우팅

워커는 `Woker-DDP/app/worker.py`의 `poll_sqs` 함수로 메시지를 폴링합니다.

### 메시지 라우팅

```python
# Woker-DDP/app/worker.py (Lines 257-260)
elif task == "split_up":
    from pipelines import split_up
    split_up(job_details)
```

`task == "split_up"`이면 `pipelines/split_up.py`의 `split_up` 함수를 호출합니다.

---

## 3단계: split_up 파이프라인 실행

### 3.1 S3에서 원본 영상 다운로드

```python
# Woker-DDP/app/pipelines/split_up.py (Lines 132-145)
# 2. S3에서 원본 영상 다운로드
if not download_from_s3(input_bucket, input_key, source_video_path):
    send_callback(
        callback_url,
        "failed",
        "Failed to download source video from S3.",
        stage="download_failed",
        metadata={
            "job_id": job_id,
            "project_id": project_id,
            "target_lang": target_lang,
        },
    )
    return
```

**다운로드 경로:**
- S3: `s3://{input_bucket}/{input_key}`
- 로컬: `{job_id}/input/{filename}`

### 3.2 ASR (STT) 수행

```python
# Woker-DDP/app/pipelines/split_up.py (Lines 151-168)
# 3. ASR (STT) 수행
run_asr(
    job_id,
    source_video_path,
    source_lang=source_lang,
    speaker_count=speaker_count,
)

# ASR 결과물 로드
asr_result_path = paths.src_sentence_dir / COMPACT_ARCHIVE_NAME
detected_source_lang = read_transcript_language(asr_result_path)
if effective_source_lang is None and detected_source_lang:
    effective_source_lang = detected_source_lang

# compact transcript 로드하여 세그먼트 개수 확인
bundle = load_compact_transcript(asr_result_path)
segments = segment_views(bundle)
total_segments = len(segments)
```

**결과물:**
- `transcript.comp.json`: Compact transcript (세그먼트 정보 포함)
- `audio.wav`: 원본 오디오
- `vocals.wav`: 발화 음성 (보컬 분리)
- `background.wav`: 배경음
- `speaker_refs.json`: 화자 참조 정보
- `self_refs/*.wav`: 화자별 참조 오디오 샘플
- `speaker_embeddings/speaker_embeddings.json`: 화자 임베딩 (Voice replacement용)

### 3.3 청크 크기 동적 계산

```python
# Woker-DDP/app/pipelines/split_up.py (Lines 183-192)
# 청크 크기 동적 계산: job_details에서 명시적으로 제공되지 않으면 total_segments/4 사용
if initial_chunk_size is not None:
    chunk_size = initial_chunk_size
else:
    # 세그먼트 개수를 4로 나눈 값으로 청크 크기 설정 (최소 1)
    chunk_size = max(1, total_segments // 4)
    logging.info(
        f"Job {job_id}: Dynamically calculated chunk_size={chunk_size} "
        f"from total_segments={total_segments} (total_segments/4)"
    )
```

**청크 크기 결정 로직:**
- `job_details`에 `chunk_size`가 명시되어 있으면 그 값 사용
- 없으면 `total_segments // 4`로 자동 계산 (최소 1)

### 3.4 ASR 결과물 및 오디오 아티팩트 S3 업로드

```python
# Woker-DDP/app/pipelines/split_up.py (Lines 194-222)
# 4. ASR 결과물을 S3에 업로드
compact_transcript_key = (
    f"{project_prefix}/interim/{job_id}/{COMPACT_ARCHIVE_NAME}"
)
if not upload_to_s3(output_bucket, compact_transcript_key, asr_result_path):
    raise Exception("Failed to upload compact transcript to S3")

# Audio artifacts를 S3에 업로드 (헬퍼 함수 사용)
uploaded_audio = upload_audio_artifacts(
    paths, project_prefix, job_id, output_bucket
)

# s3:// 접두사 제거
audio_key = extract_s3_key(uploaded_audio.get("audio.wav"))
vocals_key = extract_s3_key(uploaded_audio.get("vocals.wav"))
background_key = extract_s3_key(uploaded_audio.get("background.wav"))
```

**업로드되는 파일:**

| 파일 | S3 경로 |
|------|--------|
| `transcript.comp.json` | `s3://{output_bucket}/{project_prefix}/interim/{job_id}/transcript.comp.json` |
| `audio.wav` | `s3://{output_bucket}/{project_prefix}/interim/{job_id}/audio/audio.wav` |
| `vocals.wav` | `s3://{output_bucket}/{project_prefix}/interim/{job_id}/audio/vocals.wav` |
| `background.wav` | `s3://{output_bucket}/{project_prefix}/interim/{job_id}/audio/background.wav` |

### 3.5 Speaker References 업로드

```python
# Woker-DDP/app/pipelines/split_up.py (Line 242)
# speaker_refs 업로드 (헬퍼 함수 사용)
upload_speaker_refs(paths, project_prefix, job_id, output_bucket)
```

**업로드되는 파일:**
- `speaker_refs.json` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/tts/speaker_refs.json`
- `self_refs/*.wav` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/tts/self_refs/*.wav`

### 3.6 Voice Replacement 준비 (선택적)

Voice replacement가 활성화된 경우 (`replace_voice_samples == True`), ASR 완료 후 자동으로 준비됩니다.

자세한 내용은 [Voice Replacement 프로세스](#voice-replacement-프로세스) 섹션을 참조하세요.

### 3.7 청크 생성

```python
# Woker-DDP/app/pipelines/split_up.py (Lines 41-72)
def create_chunks(total_segments: int, chunk_size: int) -> list[dict]:
    """세그먼트를 청크로 분할합니다."""
    if total_segments <= 0:
        return []
    if chunk_size <= 0:
        chunk_size = CHUNK_SIZE

    chunks = []
    for i in range(0, total_segments, chunk_size):
        end_idx = min(i + chunk_size - 1, total_segments - 1)
        chunks.append(
            {
                "chunk_index": i // chunk_size,
                "start_segment_index": i,
                "end_segment_index": end_idx,
                "segment_count": end_idx - i + 1,
            }
        )
    return chunks
```

**예시:**
- 100개 세그먼트, `chunk_size=20` → 5개 청크
  - 청크 0: 세그먼트 0-19
  - 청크 1: 세그먼트 20-39
  - 청크 2: 세그먼트 40-59
  - 청크 3: 세그먼트 60-79
  - 청크 4: 세그먼트 80-99

### 3.8 Manifest.json 생성 및 업로드

```python
# Woker-DDP/app/pipelines/split_up.py (Lines 306-348)
# 6. manifest.json 생성
manifest = {
    "job_id": job_id,
    "project_id": project_id,
    "total_segments": total_segments,
    "total_chunks": total_chunks,
    "chunk_size": chunk_size,
    "chunks": chunks,
    "audio_files": {
        "compact_transcript": f"s3://{output_bucket}/{compact_transcript_key}",
    },
    "input_key": input_key,
    "input_bucket": input_bucket,
    "source_lang": effective_source_lang,
    "target_lang": target_lang,
    "output_bucket": output_bucket,
    "project_prefix": project_prefix,
    "callback_url": callback_url,
}

# audio 파일 키 추가 (있는 경우만)
if audio_key:
    manifest["audio_files"]["audio_wav"] = audio_key
if vocals_key:
    manifest["audio_files"]["vocals_wav"] = vocals_key
if background_key:
    manifest["audio_files"]["background_wav"] = background_key

# Voice replacement 정보 추가 (있는 경우만)
if voice_replacements:
    manifest["voice_replacements"] = voice_replacements
if voice_replacement_diagnostics:
    manifest["voice_replacement_diagnostics"] = voice_replacement_diagnostics

# manifest.json을 S3에 업로드
manifest_key = f"{project_prefix}/interim/{job_id}/manifest.json"
if not upload_metadata_to_s3(output_bucket, manifest_key, manifest):
    raise Exception("Failed to upload manifest.json to S3")
```

**업로드 경로:**
- `s3://{output_bucket}/{project_prefix}/interim/{job_id}/manifest.json`

### 3.9 각 청크를 chunk_work 큐에 전송

```python
# Woker-DDP/app/pipelines/split_up.py (Lines 350-402)
# 7. 각 청크를 chunk_work 큐에 전송
chunk_messages_sent = 0
for chunk in chunks:
    chunk_message = {
        "task": "chunk_work",
        "job_id": job_id,
        "project_id": project_id,
        "chunk_index": chunk["chunk_index"],
        "start_segment_index": chunk["start_segment_index"],
        "end_segment_index": chunk["end_segment_index"],
        "total_chunks": total_chunks,
        "total_segments": total_segments,
        "manifest_key": manifest_key,
        "target_lang": target_lang,
        "source_lang": effective_source_lang,
        "output_bucket": output_bucket,
        "project_prefix": project_prefix,
        "callback_url": callback_url,
        # 기타 필요한 정보 전달
        "voice_config": job_details.get("voice_config"),
        "replace_voice_samples": job_details.get("replace_voice_samples"),
        "is_replace_voice_samples": job_details.get("is_replace_voice_samples"),
        "voice_sample_substitution": job_details.get("voice_sample_substitution"),
        "voice_library_bucket": job_details.get("voice_library_bucket"),
    }
    
    # 청크별로 다른 그룹 ID 사용 (병렬 처리 가능)
    chunk_group_id = f"{project_id}_chunk_{chunk['chunk_index']}"
    message_kwargs = _build_sqs_message_kwargs(
        message_body,
        project_id=project_id,
        deduplication_id=f"{job_id}_chunk_{chunk['chunk_index']}",
        group_id=chunk_group_id,
    )
    response = sqs_client.send_message(**message_kwargs)
```

**특징:**
- 각 청크마다 `task: "chunk_work"` 메시지를 SQS에 전송
- 청크별로 다른 그룹 ID를 사용하여 병렬 처리 가능
- FIFO 큐의 경우 중복 방지를 위한 `deduplication_id` 사용

---

## 4단계: chunk_work 파이프라인 실행 (각 청크별)

각 청크는 독립적으로 병렬 처리됩니다.

### 4.1 Manifest.json 다운로드

```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 94-100)
# 2. manifest.json 다운로드
manifest_path = paths.interim_dir / "manifest.json"
if not download_from_s3(output_bucket, manifest_key, manifest_path):
    raise Exception(f"Failed to download manifest.json from {manifest_key}")

with open(manifest_path, "r", encoding="utf-8") as f:
    manifest = json.load(f)
```

### 4.2 Compact Transcript 다운로드

```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 102-120)
# 3. compact_transcript 다운로드
compact_transcript_s3_key = manifest["audio_files"]["compact_transcript"]
# s3://bucket/key 형식 처리
if compact_transcript_s3_key.startswith("s3://"):
    _, compact_transcript_key = compact_transcript_s3_key.replace("s3://", "").split("/", 1)
else:
    compact_transcript_key = compact_transcript_s3_key

compact_transcript_path = paths.src_sentence_dir / COMPACT_ARCHIVE_NAME
compact_transcript_path.parent.mkdir(parents=True, exist_ok=True)

if not download_from_s3(output_bucket, compact_transcript_key, compact_transcript_path):
    raise Exception(f"Failed to download compact_transcript from {compact_transcript_key}")
```

### 4.3 Speaker References 다운로드

```python
# Woker-DDP/app/pipelines/chunk_work.py (Line 123)
# 3.5. speaker_refs 다운로드 (헬퍼 함수 사용)
download_speaker_refs(paths, project_prefix, job_id, output_bucket, chunk_index)
```

### 4.4 청크 범위의 세그먼트 필터링

```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 125-193)
# 4. 청크 범위의 세그먼트만 필터링
bundle = load_compact_transcript(compact_transcript_path)
all_segments = segment_views(bundle)

# 청크 범위 필터링
chunk_segments = [
    seg
    for seg in all_segments
    if start_segment_index <= seg.idx <= end_segment_index
]

# 필터링된 세그먼트로 compact_transcript 재구성
filtered_bundle = dict(bundle)
original_segments = bundle.get("segments", [])
filtered_segments = []
filtered_to_original_idx_map = {}  # 필터링된 인덱스 -> 원본 인덱스 매핑

for i, seg in enumerate(original_segments):
    if start_segment_index <= i <= end_segment_index:
        filtered_idx = len(filtered_segments)
        filtered_to_original_idx_map[filtered_idx] = i  # 원본 인덱스 저장
        filtered_segments.append(seg)

filtered_bundle["segments"] = filtered_segments

# 필터링된 compact_transcript를 임시로 저장
chunk_transcript_path = (
    paths.src_sentence_dir / f"chunk_{chunk_index}_transcript.comp.json"
)
save_compact_transcript(filtered_bundle, chunk_transcript_path)

# 원본 transcript를 백업하고 청크 버전으로 교체 (임시)
original_transcript_backup = (
    paths.src_sentence_dir / f"original_{COMPACT_ARCHIVE_NAME}"
)
if compact_transcript_path.exists():
    shutil.copy(compact_transcript_path, original_transcript_backup)

# 청크 버전을 원본 위치에 복사 (번역/TTS/Sync 함수들이 이 경로를 사용)
shutil.copy(chunk_transcript_path, compact_transcript_path)
```

**중요:**
- 전체 transcript에서 청크 범위의 세그먼트만 필터링
- 필터링된 인덱스 → 원본 인덱스 매핑 생성 (번역 결과 변환용)
- 번역/TTS/Sync 함수들이 사용할 수 있도록 임시로 원본 위치에 복사

### 4.5 번역 수행

```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 195-224)
# 5. 번역 수행 (청크 범위만)
logging.info(f"Job {job_id} chunk {chunk_index}: Starting translation...")
translations = translate_transcript(job_id, target_lang, src_lang=source_lang)

# 번역 결과의 seg_idx를 원본 인덱스로 변환
for trans in translations:
    filtered_idx = trans.get("seg_idx")
    if (
        filtered_idx is not None
        and filtered_idx in filtered_to_original_idx_map
    ):
        original_idx = filtered_to_original_idx_map[filtered_idx]
        trans["seg_idx"] = original_idx

# 번역 결과를 청크별로 저장
trans_result_path = (
    paths.trg_sentence_dir / f"chunk_{chunk_index}_translated.json"
)
trans_result_path.parent.mkdir(parents=True, exist_ok=True)
with open(trans_result_path, "w", encoding="utf-8") as f:
    json.dump(translations, f, ensure_ascii=False, indent=2)

# S3에 업로드
trans_s3_key = f"{project_prefix}/interim/{job_id}/chunks/chunk_{chunk_index}/translated.json"
upload_to_s3(output_bucket, trans_s3_key, trans_result_path)
```

**업로드 경로:**
- `s3://{output_bucket}/{project_prefix}/interim/{job_id}/chunks/chunk_{chunk_index}/translated.json`

### 4.6 Voice Replacement 준비 (manifest에서 읽기)

```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 227-278)
# 6. Voice replacement 준비 (manifest에서 읽기)
speaker_voice_overrides: dict[str, dict] = {}
if replace_voice_samples and "voice_replacements" in manifest:
    voice_replacements = manifest["voice_replacements"]
    
    # S3에서 voice replacement 파일들 다운로드
    asset_dir = paths.interim_dir / "voice_replacements"
    asset_dir.mkdir(parents=True, exist_ok=True)
    
    for speaker, replacement_info in voice_replacements.items():
        # S3에서 다운로드
        if "s3_key" in replacement_info:
            local_path = (
                asset_dir / f"{speaker}_{replacement_info['voice_id']}.wav"
            )
            if not local_path.exists():
                s3_bucket = replacement_info.get("s3_bucket", output_bucket)
                if download_from_s3(
                    s3_bucket, replacement_info["s3_key"], local_path
                ):
                    logging.debug(
                        f"Job {job_id} chunk {chunk_index}: "
                        f"Downloaded voice replacement for {speaker} from S3"
                    )
            # 로컬 경로로 업데이트
            replacement_info["audio_path"] = str(local_path)
            speaker_voice_overrides[speaker] = replacement_info
```

### 4.7 TTS 수행

```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 280-317)
# 7. TTS 수행 (청크 범위만)
logging.info(f"Job {job_id} chunk {chunk_index}: Starting TTS...")
user_voice_sample_path = None
if (
    voice_config
    and voice_config.get("kind") == "s3"
    and voice_config.get("key")
):
    voice_key = voice_config["key"]
    voice_bucket = (
        voice_config.get("bucket")
        or voice_config.get("bucket_name")
        or output_bucket
    )
    user_voice_sample_path = paths.interim_dir / Path(voice_key).name
    download_from_s3(voice_bucket, voice_key, user_voice_sample_path)

segments_payload = generate_tts(
    job_id,
    target_lang,
    voice_sample_path=user_voice_sample_path,
    speaker_voice_overrides=(
        speaker_voice_overrides if speaker_voice_overrides else None
    ),
)

# TTS 메타데이터만 업로드 (최적화: 실제 오디오 파일은 sync 후 업로드)
tts_dir = paths.vid_tts_dir
segments_json_path = tts_dir / "segments.json"
if segments_json_path.exists():
    segments_json_key = (
        f"{project_prefix}/interim/{job_id}/chunks/chunk_{chunk_index}/"
        f"segments.json"
    )
    upload_to_s3(output_bucket, segments_json_key, segments_json_path)
```

**업로드 경로:**
- `s3://{output_bucket}/{project_prefix}/interim/{job_id}/chunks/chunk_{chunk_index}/segments.json`

### 4.8 Sync 수행

```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 319-384)
# 8. Sync 수행 (청크 범위만)
logging.info(f"Job {job_id} chunk {chunk_index}: Starting sync...")
try:
    synced_segments = sync_segments(job_id)
except FileNotFoundError as exc:
    logging.warning(
        f"Job {job_id} chunk {chunk_index}: Sync artifacts not found: {exc}"
    )
    synced_segments = []
except Exception as exc:
    logging.warning(f"Job {job_id} chunk {chunk_index}: Sync failed: {exc}")
    synced_segments = []

# Global Index 복원: 청크 내부 인덱스를 전체 영상 기준 인덱스로 변환
if synced_segments and start_segment_index is not None:
    for i, seg in enumerate(synced_segments):
        global_idx = start_segment_index + i
        seg["seg_idx"] = global_idx
        seg["segment_index"] = global_idx
        seg["segment_id"] = f"segment_{global_idx:04d}"

# Sync 결과물을 S3에 업로드
synced_dir = paths.vid_tts_dir / "synced"
if synced_dir.exists():
    for sync_file in synced_dir.glob("**/*"):
        if not sync_file.is_file():
            continue
        # mux를 위한 표준 경로로만 업로드 (중복 제거)
        sync_key = f"{project_prefix}/interim/{job_id}/text/vid/tts/synced/{sync_file.name}"
        upload_to_s3(output_bucket, sync_key, sync_file)

# segments_synced.json도 업로드
synced_meta_path = synced_dir / "segments_synced.json"
if synced_segments:
    with open(synced_meta_path, "w", encoding="utf-8") as f:
        json.dump(synced_segments, f, ensure_ascii=False, indent=2)

if synced_meta_path.exists():
    synced_meta_key = (
        f"{project_prefix}/interim/{job_id}/chunks/chunk_{chunk_index}/"
        f"segments_synced.json"
    )
    upload_to_s3(output_bucket, synced_meta_key, synced_meta_path)
```

**업로드 경로:**
- Sync된 오디오 파일들: `s3://{output_bucket}/{project_prefix}/interim/{job_id}/text/vid/tts/synced/{filename}.wav`
- `segments_synced.json`: `s3://{output_bucket}/{project_prefix}/interim/{job_id}/chunks/chunk_{chunk_index}/segments_synced.json`

**중요:**
- 청크 내부 인덱스를 전체 영상 기준 인덱스(Global Index)로 변환
- 모든 청크의 sync 결과물이 동일한 경로(`tts/synced/`)에 업로드되어 mux 단계에서 쉽게 접근 가능

### 4.9 원본 Transcript 복원

```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 386-388)
# 9. 원본 transcript 복원
if original_transcript_backup.exists():
    shutil.copy(original_transcript_backup, compact_transcript_path)
```

### 4.10 완료 체크 및 Mux 트리거

```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 390-400)
# 10. 완료 체크 및 Mux 트리거
check_and_trigger_mux_if_complete(
    job_id,
    project_id,
    total_segments,
    output_bucket,
    project_prefix,
    callback_url,
    job_details,
    send_callback_func=send_callback,
)
```

**동작:**
- S3에서 `tts/synced/` 디렉토리의 `.wav` 파일 개수 확인
- `wav_count >= total_segments`이면 모든 청크 완료로 판단
- 중복 실행 방지를 위한 락 파일 생성
- `task: "mux"` 메시지를 SQS에 전송

---

## S3 다운로드/업로드 요약

### split_up 단계

#### S3 다운로드
- 원본 영상: `s3://{input_bucket}/{input_key}`

#### S3 업로드
- `transcript.comp.json` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/transcript.comp.json`
- `audio.wav` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/audio/audio.wav`
- `vocals.wav` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/audio/vocals.wav`
- `background.wav` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/audio/background.wav`
- `speaker_refs.json` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/tts/speaker_refs.json`
- `self_refs/*.wav` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/tts/self_refs/*.wav`
- `voice_replacements/*.wav` (선택적) → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/voice_replacements/{speaker}_{voice_id}.wav`
- `manifest.json` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/manifest.json`

#### SQS 메시지 전송
- 각 청크마다 `task: "chunk_work"` 메시지 전송

### chunk_work 단계 (각 청크별)

#### S3 다운로드
- `manifest.json` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/manifest.json`
- `transcript.comp.json` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/transcript.comp.json`
- `speaker_refs.json` 및 `self_refs/*.wav` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/tts/`
- `voice_replacements/*.wav` (선택적) → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/voice_replacements/`
- 사용자 음성 샘플 (선택적) → `s3://{voice_bucket}/{voice_key}`

#### S3 업로드
- `translated.json` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/chunks/chunk_{chunk_index}/translated.json`
- `segments.json` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/chunks/chunk_{chunk_index}/segments.json`
- Sync된 오디오 파일들 → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/text/vid/tts/synced/{filename}.wav`
- `segments_synced.json` → `s3://{output_bucket}/{project_prefix}/interim/{job_id}/chunks/chunk_{chunk_index}/segments_synced.json`

---

## Voice Replacement 프로세스

Voice replacement는 원본 화자의 음성 특성을 분석하여 타겟 언어의 유사한 음성으로 자동 교체하는 기능입니다.

### split_up 단계에서의 Voice Replacement

#### 1. 플래그 확인
```python
# Woker-DDP/app/pipelines/split_up.py (Lines 94-103)
voice_replacement_flag = (
    job_details.get("replace_voice_samples")
    or job_details.get("is_replace_voice_samples")
    or job_details.get("voice_sample_substitution")
)
replace_voice_samples = _parse_bool(voice_replacement_flag)
```

#### 2. Voice Replacement 준비
```python
# Woker-DDP/app/pipelines/split_up.py (Lines 244-296)
if replace_voice_samples:
    overrides, diagnostics = maybe_prepare_voice_replacements(
        paths, target_lang, voice_library_bucket or output_bucket
    )
    voice_replacements = overrides
    voice_replacement_diagnostics = diagnostics
```

**`maybe_prepare_voice_replacements` 함수 동작:**

1. **Speaker Embeddings 로드**
   - ASR에서 생성된 `speaker_embeddings/speaker_embeddings.json` 로드
   - 없으면 `missing_embeddings`로 종료

2. **Voice Library 인덱스 다운로드 및 로드**
   - 타겟 언어의 Voice Library 인덱스를 S3에서 다운로드
   - 경로: `voice-samples/embedding/{lang}/{lang}.json`
   - 없으면 `library_unavailable`로 종료

3. **Voice 추천 (Cosine Similarity)**
   - 각 화자 임베딩과 Voice Library의 임베딩 간 코사인 유사도 계산
   - 가장 유사한 음성을 매칭
   - 매칭 없으면 `no_matches`로 종료

4. **Voice Replacement 샘플 다운로드 (Materialization)**
   - 매칭된 음성 샘플을 S3에서 로컬로 다운로드
   - `{interim_dir}/voice_replacements/{speaker}_{voice_id}.wav`에 저장
   - 다운로드 실패 시 `materialization_failed`로 종료

#### 3. Voice Replacement 파일 S3 업로드
```python
# Woker-DDP/app/pipelines/split_up.py (Lines 258-281)
if voice_replacements:
    for speaker, replacement_info in voice_replacements.items():
        audio_path = Path(replacement_info["audio_path"])
        if audio_path.exists() and audio_path.is_file():
            replacement_key = (
                f"{project_prefix}/interim/{job_id}/voice_replacements/"
                f"{speaker}_{replacement_info['voice_id']}.wav"
            )
            if upload_to_s3(output_bucket, replacement_key, audio_path):
                voice_replacements[speaker]["s3_key"] = replacement_key
                voice_replacements[speaker]["s3_bucket"] = output_bucket
```

#### 4. Manifest에 Voice Replacement 정보 추가
```python
# Woker-DDP/app/pipelines/split_up.py (Lines 335-339)
if voice_replacements:
    manifest["voice_replacements"] = voice_replacements
if voice_replacement_diagnostics:
    manifest["voice_replacement_diagnostics"] = voice_replacement_diagnostics
```

### chunk_work 단계에서의 Voice Replacement

#### 1. Manifest에서 Voice Replacement 정보 읽기
```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 227-278)
if replace_voice_samples and "voice_replacements" in manifest:
    voice_replacements = manifest["voice_replacements"]
    
    # S3에서 voice replacement 파일들 다운로드
    for speaker, replacement_info in voice_replacements.items():
        if "s3_key" in replacement_info:
            local_path = (
                asset_dir / f"{speaker}_{replacement_info['voice_id']}.wav"
            )
            if not local_path.exists():
                s3_bucket = replacement_info.get("s3_bucket", output_bucket)
                download_from_s3(s3_bucket, replacement_info["s3_key"], local_path)
            replacement_info["audio_path"] = str(local_path)
            speaker_voice_overrides[speaker] = replacement_info
```

#### 2. TTS에 Voice Replacement 적용
```python
# Woker-DDP/app/pipelines/chunk_work.py (Lines 297-304)
segments_payload = generate_tts(
    job_id,
    target_lang,
    voice_sample_path=user_voice_sample_path,
    speaker_voice_overrides=(
        speaker_voice_overrides if speaker_voice_overrides else None
    ),
)
```

---

## 주요 특징

1. **병렬 처리**: 각 청크는 독립적으로 병렬 처리되어 전체 처리 시간 단축
2. **동적 청크 크기**: 세그먼트 개수에 따라 자동으로 청크 크기 계산
3. **Voice Replacement**: 원본 화자와 유사한 타겟 언어 음성으로 자동 교체
4. **Global Index 관리**: 청크별 처리 후 전체 영상 기준 인덱스로 변환하여 일관성 유지
5. **S3 기반 저장**: 모든 중간 결과물을 S3에 저장하여 워커 간 공유 가능

---

## 참고 파일

- `Woker-DDP/app/pipelines/split_up.py`: Split up 파이프라인
- `Woker-DDP/app/pipelines/chunk_work.py`: Chunk work 파이프라인
- `Woker-DDP/app/worker.py`: 워커 메인 파일
- `backend/app/api/jobs/service.py`: 백엔드 메시지 구성
- `Woker-DDP/app/services/voice_recommendation.py`: Voice replacement 로직

