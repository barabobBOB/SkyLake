# 프로젝트 목적

## 프로젝트 개요

SkyLake는 Bluesky Jetstream 이벤트를 실시간으로 수집하고, Apache Kafka, Apache Flink, Apache Iceberg를 활용해 Kubernetes 위에서 처리하는 실시간 Lakehouse 프로젝트다.

목표는 단순히 데이터를 수집하는 것이 아니라, 원본 데이터 보존, event-time 처리, 중복 이벤트, late-arriving event, malformed record, checkpoint 복구, 저장소 관리, 관측성 같은 실제 데이터 엔지니어링 문제를 다루는 작은 실시간 데이터 플랫폼을 직접 운영하는 것이다.

## 핵심 목표

- Bluesky Jetstream WebSocket 이벤트를 실시간으로 수집한다.
- Apache Kafka를 통해 raw event를 버퍼링한다.
- Apache Flink로 이벤트 스트림을 처리한다.
- 원본 및 처리된 데이터를 Apache Iceberg 테이블에 저장한다.
- Bronze, Silver, Gold 데이터 레이어를 설계한다.
- 되돌릴 수 없는 변환을 수행하기 전에 raw JSON을 보존한다.
- watermark, deduplication, partitioning, DLQ 정책을 실제 데이터 프로파일링 결과로 결정한다.
- Kubernetes 위에서 persistent storage와 node-aware placement를 고려해 파이프라인을 운영한다.
- 통제된 장애 실험으로 복구 동작을 검증한다.

## 설계 배경

이 프로젝트는 ClickHouse의 Bluesky Medallion Architecture 사례를 참고했다. 이 사례에서 중요한 것은 특정 데이터베이스 선택이 아니라 다음과 같은 데이터 엔지니어링 접근이다.

- Bluesky 이벤트는 semi-structured JSON이다.
- 이벤트 타입마다 JSON path가 다르다.
- 이벤트 timestamp가 서로 다른 필드에 존재할 수 있다.
- event time과 ingestion time 사이에 차이가 발생할 수 있다.
- 중복 이벤트가 존재할 수 있다.
- 잘못된 레코드나 지나치게 늦게 도착한 레코드는 조용히 버리지 않고 분리해야 한다.
- 분석용 테이블을 만들기 전에 raw 데이터를 보존해야 한다.

SkyLake는 이 문제 해결 방식을 Kafka/Flink/Iceberg 기반 실시간 Lakehouse 아키텍처로 재해석한다.

## 목표 아키텍처

```text
Bluesky Jetstream
  -> Bluesky Producer
  -> Kafka
  -> Flink Streaming Jobs
  -> Apache Iceberg on MinIO
       - Bronze: raw JSON
       - Silver: normalized events
       - Gold: aggregated metrics
  -> Trino / SQL analysis
  -> Grafana / Superset
```

## 데이터 레이어 목표

### Bronze

Bronze는 raw event와 ingestion metadata를 저장한다.

Bronze는 다음 목적에 사용된다.

- raw JSON 보존
- 데이터 프로파일링
- replay 및 backfill
- schema evolution 분석
- downstream 처리 실패 시 복구 기준 데이터

### Silver

Silver는 parsing과 normalization이 완료된 이벤트를 저장한다.

Silver는 다음 처리를 담당한다.

- JSON parsing
- event timestamp 추출
- actor 및 collection 추출
- ingestion latency 계산
- duplicate detection
- late event 처리
- DLQ 분리

Silver 처리 규칙은 추측이 아니라 Bronze profiling 결과를 바탕으로 결정한다.

### Gold

Gold는 분석과 대시보드에서 바로 사용할 수 있는 aggregate table을 저장한다.

예상 지표:

- 분당 이벤트 수
- collection별 이벤트 수
- 일별 활성 사용자 수
- 언어별 트렌드
- ingestion latency
- duplicate rate
- late event rate
- DLQ rate

## 운영 목표

SkyLake는 단순 코드 데모가 아니라 운영 경험을 쌓기 위한 프로젝트로 설계한다.

프로젝트에서 다룰 운영 항목은 다음과 같다.

- Kubernetes namespace와 workload placement
- stateful service를 위한 PVC 관리
- Kafka retention 관리
- Flink checkpoint와 savepoint
- Iceberg snapshot 및 metadata 관리
- MinIO object storage 증가량 관리
- Trino 기반 profiling query
- Grafana와 Prometheus 기반 monitoring
- runbook 및 incident log 작성

## 장애 검증 목표

프로젝트에는 작고 통제된 장애 주입 실험을 포함한다.

- producer 재시작
- broker 재시작
- Flink TaskManager 종료
- MinIO 일시 중단
- malformed JSON 투입
- late event 투입
- duplicate event 투입

각 실험은 다음 형식으로 기록한다.

```text
Experiment:
Hypothesis:
Procedure:
Observed Behavior:
Data Correctness Check:
Follow-up Action:
```

## 최종 프로젝트 설명

SkyLake는 Bluesky Jetstream 이벤트를 대상으로 Kubernetes 기반 실시간 Lakehouse 파이프라인을 구축하고 운영하는 프로젝트다. Kafka로 이벤트를 수집하고, Flink의 event-time semantics로 처리한 뒤, Apache Iceberg Bronze/Silver/Gold 테이블에 저장한다. 또한 monitoring, data profiling, failure recovery experiment를 통해 운영 동작과 데이터 정합성을 검증한다.
