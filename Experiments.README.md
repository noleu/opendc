# Experiments

Two types of experiments have been conducted in this project: validation and report. The validation experiments are used to validate the correctness of the implementation.
The validation experiments are used to evaluate the correct implementation of the scheduling algorithms. The results are described in the `Validation` appendix of the report.
While the report experiments are used to study the characteristics of the scheduling algorithms. The results are described in the `Experiments` section of the report.

We assume that OpenDC has been downloaded, and compiled. For executing the experiments the IntelliJ idea has been used.
Executing the binary in the corresponding way should lead to the same results, but is not verified.

The algorithms from the report correspond to the implementation in the following way:

- Cheap if possible -> Price
- Threshold-based -> RadicalPrice
- Greedy -> Greedy
- Uniform Progression -> UniformProgression
- Intelligent Bidding -> IntelligentBidding

## Validation Experiments

The validation experiments are located in the 'TestExperiments' folder, including the files to create the parquet files and the plots.

### Recreate parquet files

To recreate the parquet files, the parquet_helper.py in the 'TestExperiments' folder can be used. The parquet_helper.py script is used to create the parquet files from the output of the validation experiments.
Either python version 3.12 can be used, for any other version it is unknown if it is working. The corresponding dependencies need to be installed and can be found in the first lines of the file.

### Executing the experiments

To run the experiments, the 'TestExperiments' folder needs to be the working directory.
Then to validate the correctness of a specific scheduling algorithm, the corresponding experiments files need to be given as input parameter to OpenDC.
They can be found in the 'experiments' subfolder. The topologies are located in the 'topologies' subfolder.
The validate subfolder of the traces folder, contains the workload and the price traces.
The results are then stored in the 'output' folder.

### Plotting the results

To plot the experiments Python version 3.12 is guaranteed to work.
The files to plot the validation graphs in the report can be found in the 'Plotting' folder.
Requirements can be found in requirements.txt.
All validation plots were created using the validation.ipynb file.

## Report Experiments

The report experiments are located in the 'Experiments' folder, including the files to create the parquet files and the plots.

### Recreate parquet files

To recreate the parquet files, the parquet_helper.py in the 'TestExperiments' folder can be used. The parquet_helper.py script is used to create the parquet files from the output of the validation experiments.
Either python version 3.12 can be used, for any other version it is unknown if it is working. The corresponding dependencies need to be installed and can be found in the first lines of the file.

### Executing the experiments

To run the experiments, the 'Experiments' folder needs to be the working directory.
Then to validate the correctness of a specific scheduling algorithm, the corresponding experiments files need to be given as input parameter to OpenDC.
They can be found in the 'experiments' subfolder. The topologies are located in the 'topologies' subfolder.
With the price_traces as a subfolder of the topologies folder, containing the price traces. The workload are located in the 'workloads' subfolder.
The results are then stored in the 'output' folder.

### Plotting the results

To plot the experiments Python version 3.12 is guaranteed to work.
The files to plot the experiment graphs in the report can be found in 'Plotting' folder
Requirements can be found in requirements.txt.
Pricing plots were created using the pricing.ipynb file. This file generates Figures 2, 4 - 8.
Runtime plots were created using the runtime.ipynb file. This file generates Figure 3.
Details can be seen in the files.
