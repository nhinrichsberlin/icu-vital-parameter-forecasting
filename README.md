### Vital parameter forecasting in the ICU

This repository contains the software
behind the paper entitled **Short-term vital parameter
forecasting in the intensive care unit - A benchmark study 
leveraging data from patients after cardiothoracic surgery**.

- The _R_ folder contains code in the R statistical 
  programming language relating to uni-variate forecasting models
  and to the computation and visualisation of error metrics.
- The _python_ folder contains Python scripts used for data 
  processing and training of multivariate neural network models
  using PyTorch.
- For data privacy reasons, the _data_ folder does not contain
  any actual patient data, 
  and merely represents the folder structure as referred to in
  the R and Python scripts. The publication is based on two
  data sources: ICU data from a German Heart Center, and the eICU
  collaborative research database (https://eicu-crd.mit.edu/).
