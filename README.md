# Introduction
This repository contains various artifacts, such as source code, experimental results, and other materials, that supplement our work on **Synthesizing Third Normal Form Schemata that Minimize Integrity Maintenance and Update Overheads: Parameterizing 3NF by the Numbers of Minimal Keys and Functional Dependencies**.\
&nbsp;&nbsp;&nbsp;&nbsp;Foremost, the repository contains an implementation of the lossless, dependency-preserving decomposition algorithm that minimizes the number of keys on subschemata in BCNF and the number of non-key FDs on subschemata in 3NF. Variant A1: <kbd>src/nf/iCONFOpt_minf_maxk.java</kbd> breaks further ties between redundant 3NF subschemata (in case they have the same number of non-key FDs) by prioritizing those with a higher number of minimal keys, while variant A2: <kbd>src/nf/iCONF.java</kbd>) only breaks ties using the number of non-key FDs. We have also implemented an additional three algorithms that allow us to compare our algorithms with the previous state-of-the-art (SOTA) algorithm CONF: A3(<kbd>src/nf/CONF.java</kbd>); as well as BC-Cover: A4(<kbd>src/nf/DecompAlg2.java</kbd>) and Synthesis: A5(<kbd>src/nf/DecompAlg4.java</kbd>). We have also included the code of implementations for other experiments: <kbd>src/exp/</kbd> and <kbd>src/util/</kbd>. For all experimental results, logs and some SQL scripts are included, too (<kbd>Artifact/Experiments/</kbd>). In the following sections, we describe how our experiments can be reproduced. 
# Preliminaries: Getting databases ready for experiments
> 1. Import 12 datasets as SQL databases
>> We have used MySQL 8.0.29 as database workbench. Firstly, please create a database. Afterwards, import the [12 datasets](https://hpi.de/naumann/projects/repeatability/data-profiling/fds.html) as MySQL databases by setting column names as 0,1,...,n-1 where n is the number of columns in a given dataset. In addition, please create a column named "id" as an auto_increment attribute for each table that will facilitate the removal of updated tuples quickly.
> 2. Import TPC-H benchmark
>> Please visit the [website](https://relational.fit.cvut.cz/dataset/TPCH) and export the TPC-H database as an .sql file. Then, please import the file in your own local MySQL workbench. Under <kbd>Artifact/Experiments/TPCH/</kbd> we have included all 22 official SQL queries and refresh functions for use in our experiments.
>3. Functional dependencies (FDs)
>> For each of the 12 datasets, the atomic closure for the set of FDs that hold on a dataset are given as separate json files in <kbd>Artifact/FD/</kbd>. For the TPC-H benchmark, the FDs(including keys) are in <kbd>Artifact/Experiments/TPCH/TPCH_schemata(1st exp).txt or TPCH_schemata(2nd exp).txt</kbd>.
>4. JDK & JDBC
>> Our code was developed and run in JAVA with version 17.0.7. At the same time, we used JDBC (version 8.0.26) as a connector to access MySQL databases.
# Experiments
In line with our paper, our experiments are organized into four sections. For each of them, you can run different code/scripts:
>1. Mini Study
>> In this part, we conducted a mini-study to showcase our research motivation as part of the introduction. Using the same FDs as input, iCONF (our new algorithm) and CONF (previous SOTA) produce two different decompositions. For each decomposition, we insert projections of the same records over the original schema on each subschema of the decomposition. The mini-study shows that 1) the decomposition from iCONF reduces update overheads much more than CONF does, illustrating that non-key FDs are a bottleneck for integrity maintenance, but also that 2) the use of FDs to uniformly do integrity checking for minimal keys and non-key FDs is prohibitively expensive. In particular, 2) also motivates our new framework where 3NF normalization is recast in terms of minimal keys, non-key FDs, and integrity maintenance. To reproduce the mini-study experiment, you can set up some parameters and run the code at <kbd>src/exp/SyntheticExpForCaseStudy.java</kbd>.
>2. How do keys and non-key FDs affect performance?
>> In this experiment, we ran the entire TPC-H benchmark (scaling factor 0.1) with the 22 queries, 7 refreshes and 3 inserts (adding 1k, 2k, and 3k of records) operations after declaring 1 to 5 minimal keys as UNIQUE constraints and enforcing 1 to 5 non-key FDs by triggers on each table. Through the workload experiments, our TPC-H study emphasizes the need to reduce the tremendous overhead caused by non-key FDs during updates. To reproduce the experiment, you can run the code at <kbd>src/exp/TPCHWorkloadExp.java</kbd>.
>3. How good are our algorithms?
>> We show the efficiency and effectiveness of our algorithms in this section. The experiments illustrate what our algorithms achieve over SOTA at the logical level, particularly in terms of reducing the number of non-key FDs on critical subschemata. The decomposition algorithms can be run at the same time by using the code in <kbd>src/exp/DecompExp.java</kbd>, or you can run each decomposition algorithm separately using the code in <kbd>src/nf/</kbd>.
>4. How much overhead do we save?
>> Finally, we have illustrated how much update overhead our algorithms save. For that purpose, we insert 10k, 20k, and 30k of records into hepatitis, abalone, ncvoter, lineitem, and weather. These insertions are done for the projections of these records onto the output schemata of our decompositions, resulting from various variants of our algorithms: iConf-f>k (A1: minimizes the number of non-key FDs first, then maximizes the number of minimal keys when ties still exist), iConf-f<k (minimizes the number of non-key FDs first, then minimizes the number of minimal keys when ties still exist), iConf->kf (maximizes the number of minimal keys first, then minimizes the number of non-key FDs when ties still exist), iConf-f (A2: minimizes the number of non-key FDs), CONF, BC-Cover, and Synthesis. To reproduce the experiment, you can run the code at <kbd>src/exp/SubschemaPerfExp.java</kbd>.
# How to run code from the command line
1. Clone the repository:
   ```bash
   git clone https://github.com/zzxhelloworld/iCONF.git
   ```
2. Navigate to the project directory:
   ```bash
   cd your_project_directory
   ```
3. Run separate code from the command line for experiments:
   
   3.1 Mini Study
   ```bash
   javac SyntheticExpForCaseStudy.java
   java SyntheticExpForCaseStudy <output_path> <db_table_name> <experiment_repeat_num> <synthetic_dataset_num> <insert_num>
   ```
   3.2 How do keys and non-key FDs affect performance?
   ```bash
   javac TPCHWorkloadExp.java
   java TPCHWorkloadExp <experiment_repeat_num> <TPCH_sql_path> <TPCH_schema_output_path> <experimental_result_output_path>
   ```
   3.3 How good are our algorithms?
   ```bash
   javac DecompExp.java
   java DecompExp <dataset_name> <experimental_results_output_directory>
   ```
   3.4 How much overhead do we save?
   ```bash
   javac SubschemaPerfExp.java
   java SubschemaPerfExp <experiment_repeat_num> <schema_sample_num> <experimental_results_output_path>
                         <decomposition_algs_separated_by_commas> <dataset_name> <experimental_results_output_directory>
   ```

   

