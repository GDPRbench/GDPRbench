# GDPRbench

The General Data Protection Regulation (GDPR) was introduced in Europe to offer new rights and protections to people concerning their personal data. In this project, we aim to benchmark how well a given storage system responds to the common queries of GDPR. In order to do this, we identify four key roles in GDPR--customer, controller, processor, and regulator--and compose workloads corresponding to their functionalities. The design of our benchmark is guided by our analysis of GDPR as well as the usage patterns from the real-world.

## Design and Implementation

We implement GDPRbench by adapting and extending YCSB. Figure shows the core infrastructure components of YCSB (in gray), and our modifications and extensions (in blue). We create four new workloads, a GDPR-specific workload executor, and implement DB clients (one per storage system). So far, we have added ~1300 LoC to the workload engine, and ∼400 LoC for Redis and PostgreSQL clients.

## Installation

To get started with GDPRbench, download or clone the repository. It consists of a minimal version of core YCSB together with all the functionalities of GDPRbench. Please note that you will need Maven 3 to build and use the benchmark. 

```bash
git clone https://github.com/GDPRbench/GDPRbench.git
cd GDPRbench/src/
mvn clean package
<start redis or postgres>
configure workloads/gdpr_{controller|customer|processor|regulator}
./bin/ycsb load redis -s -P workloads/gdpr_controller
./bin/ycsb run redis -s -P workloads/gdpr_controller
```
Finally, compute **T-Compliance** by running all four GDPRbench workloads, and adding up the time taken by each of them.
