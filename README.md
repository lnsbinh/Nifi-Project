# Nifi-Project

* How to build
1. Go to submit-spark-job-bundle, run mvn clean install.s
2. When the build done, go to nifi-submit-spark-job-nar/target
   copy the nar file.
3. Paste the nar file to Nifi lib directory, restart the server to apply the nar file.