import sys
# sys.path.insert(0,"/usr/local/lib/python2.7/site-packages")
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import os
from tabulate import tabulate
import numpy as np

matplotlib.style.use('ggplot')
plt.style.use("ggplot")

def save_plot(fig,save_to):
	if os.path.isfile(save_to):
		os.remove(save_to)
	fig.savefig(save_to,format="pdf", bbox_inches="tight")

def plotEvalSummary(df_plot,dataset,evaluation,output_folder,min_support,max_support):
	str_title = " ".join(["Scores for ",dataset,"evaluation",evaluation])
	img = df_plot.plot(figsize=(15,10),title=str_title,colormap="Spectral",kind='barh')
	img.legend(loc='center left', bbox_to_anchor=(1, 0.5))
	fig = img.get_figure()
	save_plot(fig,os.path.join(output_folder,evaluation + "_summary_minsupport_" + str(min_support) + "_max_support_" + str(max_support) + ".pdf"))

def plotEval(df_plot,bin_size,dataset,evaluation,output_folder,freq_type,min_support,max_support):
	str_title = evaluation + " vs. the support of the conditioned item " + freq_type + " in P(j|i) for " + dataset + " dataset"
	df_grouped = df_plot.groupby(pd.cut(df_plot[freq_type], bin_size)).mean().dropna()
	df_grouped.drop([freq_type],inplace=True,axis=1)
	img = df_grouped.plot(figsize=(15,10),title=str_title,colormap="Spectral")
	img.legend(loc='center left', bbox_to_anchor=(1, 0.5))
	fig = img.get_figure()
	save_plot(fig,os.path.join(output_folder,evaluation + "_plot_minsupport_" + str(min_support) + "_max_support_" + str(max_support) + ".pdf"))

def createSeries(line):
	#Check if it is the splitted version of the log
	l_split = line.split(",")
	if "EVALUATION_RESULT" in l_split[0]:
		l = line.split(",")[1:]
	else:
		l = line.split(",")[3:]
	result = {}
	result["i"] = l[0].split("(")[2]
	result["j"] = l[1].split(")")[0]
	result["pair_freq_test"] = float(l[2].split(")")[0])
	result["pair_freq_train"] = float(l[3])
	result["freq_i_train"] = float(l[4])
	result["freq_i_test"] = float(l[5])
	result["freq_j_train"] = float(l[6])
	result["freq_j_test"] = float(l[7])
	for i in range(9,len(l)):
		result[l[i].split(" -> ")[0].strip()] = float(l[i].split(" -> ")[1].strip())
	return pd.Series(result)

def main(dataset,file_path,min_support,max_support,bin_size,output_folder,freq_type):
	with open(file_path, "r") as text_file:
		lines = text_file.readlines()
		eval_lines = [line for line in lines if "EVALUATION_RESULT" in line]
		pd_log = pd.DataFrame(eval_lines,columns=["line"])
		if freq_type == 'freq_i_train':
			pd_eval = pd_log["line"].apply(lambda x: createSeries(x)).query("freq_i_train >= @min_support and freq_i_train <= @max_support")
		if freq_type == 'freq_i_test':
			pd_eval = pd_log["line"].apply(lambda x: createSeries(x)).query("freq_i_test >= @min_support and freq_i_test <= @max_support")
		if freq_type == 'freq_j_train':
			pd_eval = pd_log["line"].apply(lambda x: createSeries(x)).query("freq_j_train >= @min_support and freq_j_train <= @max_support")
		if freq_type == 'freq_j_test':
			pd_eval = pd_log["line"].apply(lambda x: createSeries(x)).query("freq_j_test >= @min_support and freq_j_test <= @max_support")

		pd_eval.to_csv(os.path.join(output_folder,"detailed_eval_minsupport_") + str(min_support) + "_maxsupport_" + str(max_support) + ".csv",index=False)

		pd_eval_total = sum(pd_eval["pair_freq_test"])
		modalities = np.unique([c.split("_")[-1] for c in pd_eval.columns if c not in ["i","j","pair_freq_test","pair_freq_train","freq_i_train","freq_i_test","freq_j_train","freq_j_test"]])

		for e in modalities:
			print "------------------------"
			print str(len(pd_log)) + " pairs for " + e
			print "------------------------"
			results = []
			for column in [column for column in pd_eval.columns if e in column and column not in ["i","pair_freq_test","j","freq_i_train"]]:
				res = {"modality":column,"score": sum(pd_eval[column] * pd_eval["pair_freq_test"]) / pd_eval_total}
				results.append(res)
				#print column + ": " + str(sum(pd_eval[column] * pd_eval["freq"]) / pd_eval_total)
				#print "------------------------"
			pd_results = pd.DataFrame(results)
			print tabulate(pd_results.sort_values(by=["score"],ascending=True),headers='keys', tablefmt='psql')

			pd_results.sort_values(by=["score"],ascending=True).to_csv(os.path.join(output_folder,e + "_results_") + str(min_support) + "_maxsupport_" + str(max_support) + ".csv",index=False)

			plotEvalSummary(pd_results.set_index("modality").sort_values(by=["score"],ascending=True),dataset,e,output_folder,min_support,max_support)

			columns = [c for c in pd_eval.columns if e in c and c not in ["i","j","pair_freq_test","pair_freq_train","freq_i_train","freq_i_test","freq_j_train","freq_j_test"]]
			columns.append(freq_type)
			df_plot_progress = pd_eval[columns]
			plotEval(df_plot_progress,bin_size,dataset,e,output_folder,freq_type,min_support,max_support)

if __name__ == "__main__":
	if len(sys.argv) <= 6 or len(sys.argv) > 7:
		print("Usage:")
		print("python check_evaluation_progress.py dataset log_file min_support max_support bin_size freq_type")
	else:
		dataset = sys.argv[1]
		log_file = sys.argv[2]
		output_folder = os.path.dirname(os.path.realpath(log_file))
		min_support = int(sys.argv[3])
		max_support = int(sys.argv[4])
		bin_size = int(sys.argv[5])
		freq_type = sys.argv[6]

		if freq_type not in ['freq_i_train','freq_i_test','freq_j_train','freq_j_test']:
			raise Exception("The frequency type should be freq_i_train or freq_i_test or freq_j_train or freq_j_test")

		if not os.path.exists(log_file):
			raise Exception("The log file: " + log_file + " does not exist")

		if not os.path.exists(output_folder):
			os.makedirs(output_folder)

		main(dataset,log_file,min_support,max_support,bin_size,output_folder,freq_type)
