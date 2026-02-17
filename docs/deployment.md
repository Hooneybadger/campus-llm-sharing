# 모델 파일과 인증 정보를 준비한 클러스터 배포

Kubernetes 클러스터에 NVIDIA 드라이버와 컨테이너 런타임, GPU device plugin이 필요합니다. 배포 설정은 노드당 GPU 한 장과 `/data/llm/models`의 모델 파일을 사용합니다. 기존 클러스터의 device plugin과 중복 설치하지 않도록 target 오버레이를 조정합니다.

## 장비에 맞춘 모델 설정

`models/registry.yaml`의 `hf_repo`에서 모델을 받아 각 노드의 `/data/llm/models/<dir>`에 저장합니다. 모델별 사용 조건과 접근 권한을 확인합니다. 서빙 컨테이너는 오프라인 로딩으로 설정되어 있으므로 모델과 토크나이저 파일을 먼저 준비해야 합니다.

소형 모델 프로파일은 32 GiB GPU의 메모리 측정값으로 계산했습니다. GPU 메모리, 모델 버전이나 실행 옵션을 바꾸면 측정값도 다시 준비합니다. 포함된 CSV는 기존 테스트베드의 계산 입력이며 새 장비의 측정값을 대신하지 않습니다.

```bash
python models/budget.py --models qwen3-4b ministral-3b phi4-mini   --kv-dtype auto --demand qwen3-4b=0.6 ministral-3b=0.2 phi4-mini=0.2   --max-model-len 4096 --max-num-seqs 32 --cold-retention 0.4 --margin-gib 1.0
```

`infra/k8s/base`의 노드 선택 조건과 `pknu.ac.kr/` 라벨은 기존 클러스터 규약입니다. 서빙 역할과 제어 서비스 역할을 실제 노드에 배정합니다. 모델 프로세스 그룹을 정리하는 복구는 모델 단위의 프로세스 장애를 다루며, GPU 장치 장애는 노드 복구가 필요합니다.

## 이미지와 외부 주소

프로젝트 루트에서 `serving/image/Dockerfile`, `gateway/image/Dockerfile`, `portal/image/Dockerfile`, `rental/image/Dockerfile`을 각각 빌드합니다. 레지스트리에 업로드한 이미지로 `infra/k8s/base`의 `registry.example.com/campus-llm-sharing/` 주소와 태그를 바꿉니다.

포털 배포의 `PUBLIC_API_BASE`와 `PUBLIC_GRAFANA_URL`은 사용자 PC에서 접근할 주소로 설정합니다. 기본 localhost 주소는 포트 포워딩용입니다. 외부 접속 경로에는 HTTPS를 구성하고 모델 서버 포트는 게이트웨이에서만 접근하게 제한합니다.

## API 키와 관리자 비밀번호

게이트웨이는 API 키의 SHA-256 해시를 사용자 정보와 연결합니다. `.secrets/gateway_keys.json`은 아래 구조로 준비합니다. `keys`의 키에는 실제 API 키의 해시를 넣습니다. API 키 원문은 사용자에게 별도로 전달합니다.

```json
{
  "keys": {
    "<SHA-256 hash>": {"user_id": "student-1", "tier": "student", "group": "class-1"}
  },
  "tiers": {
    "student": {"rpm": 30, "max_concurrent": 2, "max_tokens_cap": 2048},
    "staff": {"rpm": 120, "max_concurrent": 8, "max_tokens_cap": 8192}
  }
}
```

`.secrets/gw-admin-token`에는 별도로 생성한 관리자 토큰을 저장합니다. 두 파일은 Git 추적 대상에서 제외됩니다. Secret은 배포 매니페스트와 분리해 생성합니다.

```bash
kubectl create namespace llm-system --dry-run=client -o yaml | kubectl apply -f -
kubectl -n llm-system create secret generic gateway-keys   --from-file=.secrets/gateway_keys.json --dry-run=client -o yaml | kubectl apply -f -
kubectl -n llm-system create secret generic gw-admin-token   --from-file=token=.secrets/gw-admin-token --dry-run=client -o yaml | kubectl apply -f -
```

모니터링은 Prometheus Operator와 관련 CRD가 필요합니다. `monitoring/kps-values.yaml`을 사용해 kube-prometheus-stack을 설치하기 전에 `monitoring` 네임스페이스에 `grafana-admin` Secret을 생성합니다. 이 Secret은 `admin-user`와 `admin-password` 키를 가져야 합니다. 기본 비밀번호는 소스에 포함하지 않습니다.

## 배포와 모델 호출 검사

현재 kubectl 컨텍스트가 대상 클러스터인지 확인한 뒤 렌더링 결과를 검토합니다. `diff`의 종료 코드 1은 변경이 있다는 뜻입니다.

```bash
scripts/deploy.sh target build > /tmp/campus-manifests.yaml
scripts/deploy.sh target diff
scripts/deploy.sh target apply
```

인증 키를 `GATEWAY_API_KEY` 환경변수로 설정한 뒤 스모크 검사를 실행합니다. 이 검사는 실제 모델에 한국어 질문과 도구 호출을 보내며 GPU를 사용합니다.

```bash
python scripts/smoke.py --gateway http://localhost:30080
```

알림 규칙은 Docker와 개발 의존성이 설치된 환경에서 `scripts/test_rules.sh`로 검사합니다. 단위 테스트 통과와 실제 모델의 로딩·응답 성공을 구분해 배포 결과를 기록합니다.
