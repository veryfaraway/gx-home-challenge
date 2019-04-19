## Common

### Dir
data/input: 입력 파일

data/out/part(n): 출력 파일


#### run script option:

-b: build 후 spark 실행

(최초 실행시 jar 파일 생성을 위해서 반드시 -b 옵션을 주어야 함) 

ex) ./run-part1.sh -b



## part1
스케쥴링 툴에서 spark application parameter로 date(yyyyMMdd) 값을 전달해서 매일 단위 날짜별 밸런스 업데이트

로컬에서 실행은 run-part1.sh  


## part2
part1과 마찬가지로 스케쥴링 툴에서 spark application parameter로 date(yyyyMMdd) 값을 전달해서 매일 단위 날짜별 밸런스 업데이트

Tokens 테이블은 어떤 용도로 사용해야 하는지 이해하지 못해서 생략했습니다.

로컬에서 실행은 run-part2.sh  



