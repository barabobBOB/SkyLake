# SkyLake

Kafka, Flink, Apache Iceberg를 활용해 Kubernetes 위에서 운영하는 Bluesky 실시간 Lakehouse 파이프라인입니다.

SkyLake는 Bluesky Jetstream 이벤트를 실시간으로 수집하고, 스트리밍 처리 엔진으로 가공한 뒤, Iceberg 기반 Lakehouse에 저장하는 홈랩 데이터 엔지니어링 프로젝트입니다.

## 문서

- [프로젝트 목적](./docs/project-purpose.md)
- [아키텍처](./docs/architecture.md)
- [Phase 1 실행 계획](./docs/phase-1.md)
- [배포 전략](./docs/deployment-strategy.md)
