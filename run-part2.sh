#!/usr/bin/env bash

BUILD=0

while [[ $# -gt 0 ]]; do
    key="$1"
    case ${key} in
        -b)

            BUILD=1
      ;;
        *)
        # unknown option
        ;;
        esac
        shift
done

if [[ "$BUILD" == "1" ]]; then
    ./gradlew clean shadow
fi

spark-submit --class gx.home.challenge.part2.AccountErc20Balance \
    --master local[2] \
    build/libs/gx-home-challenge-all.jar \
    --inputContracts data/input/contracts.csv \
    --inputTokenTransfers data/input/token_transfers.csv \
    --inputTokens data/input/tokens.csv \
    -o data/out/part2 \
    --date 20160208
