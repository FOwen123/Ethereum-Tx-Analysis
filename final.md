BIG DATA PROCESSING PROJECT
Final Project / Final Exam Requirement: Distributed Data
Processing + Deep Learning Processing (Optional)
Dataset Options (Choose ONE)
Students must select one dataset / database:
1. Taiwan Traffic Data (TDCS)
o Highway traffic flow, speed, congestion
2. Open Government Data from Taiwan Open Data Portal
3. Healthcare dataset (if available from Taiwan sources)
4. Image, text, log, or time-series database for deep
learning
5. Any dataset/database > 1GB (approved by instructor)
Required Technologies
• Cloud: Amazon Web Services (AWS)
• Storage: Amazon S3
• Database: CSV/Parquet files OR SQL/NoSQL database
• Compute:
o Option 1: Apache Hadoop (MapReduce)
o Option 2: Apache Spark
o Option 3: Spark + Deep Learning framework
• Deep Learning: PyTorch / TensorFlow / Keras
• Access: AWS CLI
Project Tasks
◆ Task 1: Data Collection & Upload
• Download dataset/database (≥ 1GB)
• Upload to S3 using:
o Web console
o AWS CLI (compare performance)
• Describe data source, size, schema, and database
structure
◆ Task 2: Data Preprocessing
• Clean missing values
• Format data; convert to CSV/Parquet if needed
• Filter irrelevant attributes
• Normalize/scale features for deep learning
• Split data into train/validation/test sets
Tools:
• Python (Pandas) OR Spark
• Spark MLlib / PySpark for large-scale feature
preparation
◆ Task 3: Distributed Processing
Choose one:
Option A: Hadoop MapReduce
Option B: Spark
Output examples:
• Aggregated statistics, trends, peak hours, grouped
summaries
• Feature table prepared for the deep learning model
◆ Task 4: Cloud Integration
• Run processing on AWS:
o EC2 OR EMR
• Store cleaned data and intermediate results back to S3
• Keep clear folder structure: raw / processed /
model_input / output
◆ Task 5: Deep Learning for Database Processing (optional)
Students must build ONE deep learning model from the database:
• Classification: predict category/class from records or
images
• Regression/forecasting: predict value or future trend
• Anomaly detection: detect unusual traffic, logs, or
records
• Text/image processing: CNN, RNN/LSTM, Transformer, or
MLP
Model requirements:
• Query/extract data from database or S3 data lake
• Build DataLoader / Dataset pipeline
• Train model with PyTorch, TensorFlow, or Keras
• Save trained model, predictions, and metrics to S3
◆ Task 6: Model Evaluation & Insights
Students must answer questions like:
• What problem does the model solve?
• Which features/data fields are most useful?
• How accurate is the model?
• What patterns or predictions are discovered?
Required metrics:
• Classification: accuracy, precision, recall, F1,
confusion matrix
• Regression/forecasting: MAE, RMSE, R²
• Anomaly detection: detection rate, false alarms,
examples
Tools:
• Python (Matplotlib / Seaborn)
• OR Excel / Power BI
• Optional: Spark MLlib, TensorBoard, sklearn metrics
◆ Task 7: Visualization Dashboard
Create:
• Charts: line, bar, heatmap, confusion matrix
• Simple dashboard showing data insights + model results
◆ Task 8: Final Report & Presentation
Report Structure:
1. Introduction
2. Dataset / Database Description
3. Overall problem pipeline
4. Data Preprocessing and Distributed Processing
5. Deep Learning Model Design
6. Implementation
7. Results and Evaluation
8. Challenges
9. Conclusion
Final Exam Expected Outputs:
• Source code
• Processed data sample or output folder on S3
• Trained model file or training notebook
• Prediction results and evaluation metrics
• Presentation slides and final report
• Demo: data pipeline + model result + visualization