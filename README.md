#Getting started
* Install java jdk 1.8 and SBT
* Install python-dev, pip and virtualenv. Then run:
    * cd scripts/
    * virtualenv venv
    * source venv/bin/activate
    * pip install -r requirements.txt
* Get the datasets:
    * Movielens 1M: http://grouplens.org/datasets/movielens/
    * Netflix: http://academictorrents.com/details/9b13183dc4d60676b773c9e2cd6de5e5542cee9a
    * Yahoo! Music: https://webscope.sandbox.yahoo.com/
    * Books: http://www2.informatik.uni-freiburg.de/~cziegler/BX/
* Parse the datasets and create a csv with user_id movie rating timestamp.

#To run the experiments
* Modify the configuration file that you want to run (.json file inside scripts folder)
    * Particularly, change the locations to match your paths and the format of the user_id movie rating timestamp
* Execute the following command (e.g. movielens):
    * cd scripts
    * source venv/bin/activate
    * export JAVA_OPTS="-Xms2g -Xmx16g" 
    * python multiple_run.py dataset_movielens.json /data/experiments/movielens/ split,train,eval
    * where: 
        * dataset_movielens.json is the json file with the parameters
        * /data/experiments/movielens/ is the output folder 
        * split,train,eval are the parts of the program to be executed. 
            * 'split' generates the training and testing. 
            * 'train' generates the EIR factors
            * 'eval' computes the Item-Item Fisher conditional score (FC), the Item-item Fisher distance (FD), baselines and combinations.
    * Note that the end results are for all the dataset. Use the check_evaluation_progress.py to filter the quartiles.

#To check the progress:
* Once you see the '<step> evaluating the pairs' in the terminal you can do the following:
* cd scripts
* python check_evaluation_progress.py movielens ../target/pack/bin/logs/app.log 0 2 20 freq_i_train
    * where: 
        * movielens is the name of the dataset 
        * ../target/pack/bin/logs/app.log is the log file to check the progress
        * In case of multiple logs, cat the logs with 'cat *.log >> app_combinated.log'
        * 0 is the minimum support 
        * 300 is the maximum support (Used to limit the items that fit in a quartile) 
        * 20 is the bin size used for the plots
        * freq_i_train is the type of frequency used for the minimum support

#Notes:
* The configuration parameters "content_file" must be a json serializable to the class "ItemContent".
* The configuration parameters "jaccard_file" and "content_similarity_file" are any item-item matrix with the similarity between all the items. Check the method 'parse_similarity_file' for further information.
* Consider at least 16 GB in java heap to run the experiments. use "set JAVA_OPTS=-Xms2g -Xmx16g" before running the experiments
* The experiments sorted by running time: movielens, books, netflix, ymusic
* The software works best with multicore CPUs. By default all the cores are used.
* This software is not intended to be a product or a working recommender system. It is more about experimenting.
* Check the code multiple_run.py for information about training the EIR model and the command line parameters.
* The dataset split and the acyclic random graph for the item to item co-occurrence might affect the evaluation results. Expect similar results for the evaluations but not exact values.
* If you have any questions or issues about how to run the experiments please feel free to contact me