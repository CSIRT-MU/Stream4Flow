#Integration test for Stream4Flow
This test is designed for determining the correctness of cluster deployment.
The test consists of multiple stages.
- Run protocol statistics application on sparkMaster.
- Copy test data to producer.
- Send data once with ipfixsend to producer .
- After data are processed stored data are read and checked if expected numbers of flows matches number of flows in <b> any </b> stored data. 

<b> The test must be ran directly after cluster's deployment or there must not be any stored data in elasticsearch.</b>
Stored data could lead to an false-positive result 