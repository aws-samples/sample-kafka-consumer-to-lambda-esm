#!/usr/bin/env bash
#
# Run one demo scenario and print where to look for its evidence.
#
# Scenarios are produced from inside the VPC by the test producer Lambda, so this needs only AWS
# credentials and the CLI.
#
#   ./scripts/run_demo.sh header_only
#   ./scripts/run_demo.sh ordering shipments ShipmentDispatched
#
# Requires `terraform output` to work, i.e. run it from a directory where the pattern root has
# been applied.

set -euo pipefail

SCENARIO="${1:-matching}"
TOPIC="${2:-}"
EVENT_TYPE="${3:-}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE/terraform"

PRODUCER="$(terraform output -raw producer_function 2>/dev/null || true)"
if [[ -z "$PRODUCER" ]]; then
  echo "error: could not read producer_function from terraform output." >&2
  echo "       Has this root been applied?" >&2
  exit 1
fi

BUCKET="$(terraform output -raw failure_bucket)"

PAYLOAD="{\"scenario\":\"$SCENARIO\""
[[ -n "$TOPIC" ]] && PAYLOAD="$PAYLOAD,\"topic\":\"$TOPIC\""
[[ -n "$EVENT_TYPE" ]] && PAYLOAD="$PAYLOAD,\"event_type\":\"$EVENT_TYPE\""
PAYLOAD="$PAYLOAD}"

echo "=> invoking $PRODUCER with $PAYLOAD"
echo

RESPONSE="$(mktemp)"
trap 'rm -f "$RESPONSE"' EXIT

aws lambda invoke \
  --function-name "$PRODUCER" \
  --cli-binary-format raw-in-base64-out \
  --payload "$PAYLOAD" \
  --log-type Tail \
  --query 'LogResult' \
  --output text \
  "$RESPONSE" >/dev/null

python3 -m json.tool < "$RESPONSE"

cat <<EOF

--------------------------------------------------------------------------------
Where to look

  Projection logs (per topic):
    aws logs tail /aws/lambda/\$(terraform output -json functions | python3 -c \\
      'import json,sys; print(list(json.load(sys.stdin).values())[0])') --since 5m --follow

  Read model / ledger contents:
    aws dynamodb scan --table-name \$(terraform output -json read_model_tables | python3 -c \\
      'import json,sys; print(list(json.load(sys.stdin).values())[0])') --max-items 20

  Failed records, WITH full payloads (this is what only S3 gives you):
    aws s3 ls s3://$BUCKET --recursive
    aws s3 cp s3://$BUCKET/<key> - | python3 -m json.tool

  Filtered-out count, for the header_only and non_matching scenarios. A record dropped by the
  filter leaves no function log entry, so this metric is the only evidence it existed:
    aws cloudwatch get-metric-statistics \\
      --namespace AWS/Lambda --metric-name FilteredOutEventCount \\
      --start-time \$(date -u -v-15M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '15 min ago' +%Y-%m-%dT%H:%M:%SZ) \\
      --end-time \$(date -u +%Y-%m-%dT%H:%M:%SZ) --period 300 --statistics Sum \\
      --dimensions Name=FunctionName,Value=<function-name>
--------------------------------------------------------------------------------
EOF
