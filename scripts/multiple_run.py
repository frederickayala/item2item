import sys
sys.path.insert(0,"/usr/local/lib/python2.7/site-packages")
import pandas as pd
import csv
import numpy as np
import itertools
import random
import subprocess
import shlex
import json
import os
import numpy as np
import itertools
import glob
import matplotlib.pyplot as plt
plt.style.use('ggplot')

is_windows = "nt" in os.name


# We split our datasets into train and test subsets
# by randomly choosing a subset of users and
# placing all their item pairs in the test-set
def save_plot(fig,save_to):
    if os.path.isfile(save_to):
        os.remove(save_to)
    fig.savefig(save_to,format="pdf", bbox_inches="tight")


def splitData(df,ratio):
    all_users = np.unique(df.user.values)
    num_to_select = int(len(all_users) * ratio)
    random_users = random.sample(all_users, num_to_select)
    testing = df[df.user.isin(random_users)]
    training = df[~df.user.isin(random_users)]
    return (training,testing)


def main(datasets_location,output_dir,modes):
    with open(datasets_location) as data_file:
        datasets = json.load(data_file)

    for dataset in datasets:
        dataset_name = dataset["dataset"]
        dataset_folder = os.path.join(output_dir, dataset_name)
        training_file = os.path.join(dataset_folder, dataset_name + ".train")
        testing_file = os.path.join(dataset_folder, dataset_name + ".test")
        os.chdir("../target/pack/bin/")
        training_options = dataset["training"]
        evaluation_options = dataset["evaluation"]

        training_name = dataset_name + "_" + str(training_options["num_factors"]) + "factors_" + \
                        str(training_options["epochs"]) + "epochs_" + str(training_options["learning_rate"]) + "rate"
        training_name = training_name.replace(".","_").replace(",","_")
        dataset_training_folder = os.path.join(dataset_folder,training_name)

        if "train" in modes:
            if "split" in modes:
                os.makedirs(dataset_folder)
                print("Processing dataset: " + dataset_name)

                quote = csv.QUOTE_ALL if dataset["quote_all"] else csv.QUOTE_NONE

                if not dataset["timestamp"]:
                    df = pd.read_csv(dataset["location"],sep=dataset["separator"], quotechar=dataset["quotechar"],
                                     quoting=quote, header=None, names=["user","item","rating"],
                                     dtype={"user":str,"item":str,"rating":str})
                else:
                    df = pd.read_csv(dataset["location"],sep=dataset["separator"], quotechar=dataset["quotechar"],
                                     quoting=quote, header=None, names=["user","item","rating","timestamp"],
                                     dtype={"user":str,"item":str,"rating":str,"timestamp":str})

                print("Full dataset description:")
                print df.describe()

                df_splitted = splitData(df,dataset["ratio"])
                df_training = df_splitted[0]
                df_testing = df_splitted[1]

                print("Training description:")
                print df_training.describe()
                print("Testing description:")
                print df_testing.describe()

                if "newsreel" not in dataset_name:
                    df_training = df_training.iloc[np.random.permutation(len(df_training))].sort_values(["user"])
                    df_testing = df_testing.iloc[np.random.permutation(len(df_testing))].sort_values(["user"])
                else:
                    df_training = df_training.sort_values(["user","timestamp"])
                    df_testing = df_testing.sort_values(["user","timestamp"])

                df_training.to_csv(training_file,sep=";", quoting=csv.QUOTE_ALL,header=False,index=False,quotechar="|")
                df_testing.to_csv(testing_file,sep=";", quoting=csv.QUOTE_ALL,header=False,index=False,quotechar="|")
            # else:
            # 	df_training = pd.read_csv(training_file,sep=";", quoting=csv.QUOTE_ALL,header=False, quotechar="|")
            # 	df_training = pd.read_csv(testing_file,sep=";", quoting=csv.QUOTE_ALL,header=False, quotechar="|")

            print("* Running the Training script for: " + training_name)
            command = "sh euclidean-item-recommender-trainer"
            args = shlex.split(command)
            args.append("--datafile")
            args.append(training_file)
            args.append("--create_flink")
            args.append(str(dataset["create_flink"]))
            args.append("--consider_time")
            args.append(str(dataset["consider_time"]))
            args.append("--implicit_ratings_from")
            args.append(str(dataset["implicit_ratings_from"]))
            args.append("--filter_min_support")
            args.append(str(training_options["filter_min_support"]))
            args.append("--filter_max_support")
            args.append(str(training_options["filter_max_support"]))
            args.append("--num_factors")
            args.append(str(training_options["num_factors"]))
            args.append("--epochs")
            args.append(str(training_options["epochs"]))
            args.append("--learning_rate")
            args.append(str(training_options["learning_rate"]))
            args.append("--stop_by_likelihood")
            args.append(str(training_options["stop_by_likelihood"]))
            args.append("--training_mode")
            args.append(str(training_options["training_mode"]))
            args.append("--approx_condition")
            args.append(str(training_options["approx_condition"]))
            args.append("--max_sample")
            args.append(str(training_options["max_sample"]))
            args.append("--alpha_sampling")
            args.append(str(training_options["alpha_sampling"]))
            args.append("--output")
            args.append(dataset_training_folder)

            with open(os.path.join(dataset_folder,"train.log"), "w") as text_file:
                process = subprocess.Popen(args,stdout=subprocess.PIPE)
                for line in iter(lambda: process.stdout.readline(), ''):
                    print(line.strip())
                    text_file.write(line.strip() + "\n")

        if "eval" in modes:
            print("- Running the evaluation script for: " + training_name)
            command = "sh euclidean-item-recommender-eval"
            args = shlex.split(command)
            args.append("--factors_folder")
            args.append(dataset_training_folder)
            args.append("--training_file")
            args.append(training_file)
            args.append("--testing_file")
            args.append(testing_file)
            args.append("--create_flink")
            args.append(str(dataset["create_flink"]))
            args.append("--load_training_files")
            args.append(str(dataset["load_training_files"]))
            args.append("--consider_time")
            args.append(str(dataset["consider_time"]))
            args.append("--implicit_ratings_from")
            args.append(str(dataset["implicit_ratings_from"]))
            args.append("--filter_min_support")
            args.append(str(evaluation_options["filter_min_support"]))
            args.append("--filter_max_support")
            args.append(str(evaluation_options["filter_max_support"]))
            args.append("--filter_min_support_training")
            args.append(str(training_options["filter_min_support"]))
            args.append("--filter_max_support_training")
            args.append(str(training_options["filter_max_support"]))
            args.append("--percentile_n")
            args.append(str(evaluation_options["percentile_n"]))
            args.append("--reference_size")
            args.append(str(evaluation_options["reference_size"]))
            args.append("--modalities")
            args.append(str(evaluation_options["modalities"]))
            args.append("--fisher")
            args.append(str(evaluation_options["fisher"]))
            args.append("--mpr_sum_training")
            args.append(str(evaluation_options["mpr_sum_training"]))
            args.append("--recall_at")
            args.append(str(evaluation_options["recall_at"]))
            args.append("--dcg_at")
            args.append(str(evaluation_options["dcg_at"]))
            args.append("--list_combinations")
            args.append(str(evaluation_options["list_combinations"]))
            args.append("--normalizations")
            args.append(str(evaluation_options["normalizations"]))
            args.append("--content_file")
            args.append(str(evaluation_options["content_file"]))
            args.append("--jaccard_file")
            args.append(str(evaluation_options["jaccard_file"]))
            args.append("--content_similarity_file")
            args.append(str(evaluation_options["content_similarity_file"]))

            with open(os.path.join(dataset_folder,"eval.log"), "w") as text_file:
                process = subprocess.Popen(args,stdout=subprocess.PIPE)
                for line in iter(lambda: process.stdout.readline(), ''):
                    print(line.strip())
                    text_file.write(line.strip() + "\n")

                # if "analyze":
                # 	detail_files = glob.glob(os.path.join(dataset_training_folder,"*evaluation.csv"))
                # 	summary_files = glob.glob(os.path.join(dataset_training_folder,"*evaluation_summary.csv"))
                # 	print "Evaluating: " + str(len(detail_files)) + " detailed files"
                # 	print "Evaluating: " + str(len(summary_files)) + " summary files"

                # 	for summary_file in summary_files:
                # 		df_summary = pd.read_csv(summary_file,sep=";",quoting=csv.QUOTE_ALL,quotechar="|")
                # 		factors = list(name for name in df_summary.columns.values if "pkifactors" in name and not "fisher" in name)
                # 		modalities = list(name for name in df_summary.columns.values if "pkifactors" not in name and not "fisher" in name)
                # 		for factor in factors:
                # 			df_factor = df_summary[list(name for name in df_summary.columns.values if factor in name or name in modalities or name.replace("fisher_","") in modalities)]
                # 			df_plot = df_factor.transpose().sort([0], ascending=[0])
                # 			df_plot.columns = [factor]
                # 			img = df_plot.plot(figsize=(15,10),kind="barh",fontsize=12,title="Mean Percentile Rank")
                # 			fig = img.get_figure()
                # 			save_plot(fig,os.path.join(dataset_training_folder,factor + ".pdf"))

                # 		df_plot_summary = df_summary.transpose().sort([0], ascending=[0])
                # 		df_plot_summary.columns = [dataset_name]
                # 		height = 10 if len(df_plot_summary) <= 20 else len(df_plot_summary) * .5
                # 		img = df_plot_summary.plot(figsize=(15,height),kind="barh",fontsize=12,title="Mean Percentile Rank")
                # 		fig = img.get_figure()
                # 		save_plot(fig,os.path.join(dataset_training_folder,"summary.pdf"))

                # 	for column in df_plot_summary.columns:
                # 		arg_min = df_plot_summary[column].argmin()
                # 		min_val = df_plot_summary[column][arg_min]
                # 		print "The best score for the model: " + column + " is: " + arg_min + ": " + str(round(min_val,5))


if __name__ == "__main__":
    if len(sys.argv) <= 3 or len(sys.argv) > 4:
        print("Usage:")
        print("python multiple_run.py dataset_json_location output_dir split,train,eval")
    else:
        datasets_location = sys.argv[1]
        modes = sys.argv[3].split(",")
        output_dir = sys.argv[2]
        if sum(1 for x in (option for option in modes if option not in ["split","train","eval"])) == 0:
            if "train" in modes or "eval" in modes:
                if "split" in modes:
                    if not os.path.exists(output_dir):
                        os.makedirs(output_dir)
                    else:
                        raise Exception("The output_dir: " + output_dir + " should not exist")
                os.chdir("../")
                print("Building the latest version of the scala code")

                command = "sbt.bat pack" if is_windows else "sbt pack"

                args = shlex.split(command)
                process = subprocess.Popen(args,stdout=subprocess.PIPE)
                for line in iter(lambda: process.stdout.readline(), ''):
                    print(line.strip())
                os.chdir("./scripts/")
            main(datasets_location,output_dir,modes)
        else:
            print("Unknown option for train,eval")