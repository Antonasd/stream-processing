# Kubescale stream processing: Threshold event generator
## Building 
1. Install Java and Maven
2. Run 
    >mvn dependency:resolve
    
    and
    
    >mvn verify
 
 or just build the docker image.
## Running application
java -jar [Path to built jar file] [Input commands]

### Available commands:
* --kafka-host - Address of a kafka broker in your kafka cluster. Default localhost.
* --sla - SLA threshold (%). Default 80%.
* --davg - Average delay threshold (ms). Default 150 ms.
* --rate - Data rate threshold(mb/s). Default 50 mb/s.
* --davg_far - Threshold for average delay at the far end (ms). Default 150 ms.
* --loss_far - Threshold for the amount of packets loss at the far end (packets lost in 10 seconds). Default 20.
* --miso_far - Threshold for the amount of misordered packets at the far end (packets lost in 10 seconds). Default 200.
* --davg_near - Threshold for average delay at the far end (ms). Default 150 ms.
* --loss_near - Threshold for the amount of packets loss at the near end (packets lost in 10 seconds). Default 20.
* --miso_near - Threshold for the amount of misordered packets at the near end (packets lost in 10 seconds). Default 200.
