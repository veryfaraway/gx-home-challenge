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

spark-submit --class gx.home.challenge.part1.AccountEtherBalance \
    --master local[2] \
    build/libs/gx-home-challenge-all.jar \
    --input data/input/transactions.csv \
    --output data/out/part1 \
    --date 20151120
