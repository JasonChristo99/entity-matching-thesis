from DataFusion import fuse
import json
import matplotlib.pyplot as plt
OBSERVED = 'C:/Users/Iasonas/Downloads/MSc_DataInt_Code/Data_fusion/tutorials/cmdline_test/observed.json'
PREMATCHED = 'C:/Users/Iasonas/Downloads/MSc_DataInt_Code/Data_fusion/tutorials/cmdline_test/matched.json'
EVALUATION = 'C:/Users/Iasonas/Downloads/MSc_DataInt_Code/Data_fusion/tutorials/cmdline_test/eval.json'
#fuse_env = fuse.Fuse(verbose=True)
fuse_env = fuse.Fuse(verbose=True,global_weights=True)
# fuse_env = fuse.Fuse(verbose=True, global_weights=True, matcher_function=lambda a, b: a == b)

evaluation = json.load(open(EVALUATION, 'r'))
observed = json.load(open(OBSERVED, 'r'))

for i in range(10, 101, 10):
	portion_len = int((len(evaluation['body'])*i)/100)

	eval_portion = {'body': evaluation['body'][:portion_len]}
	json.dump(eval_portion, open('eval_{}.json'.format(i), 'w'))

	observed_portion = {'body': observed['body'][:portion_len]}
	json.dump(observed_portion, open('observed_{}.json'.format(i), 'w'))



portion_scores = {
	'recall': [],
	#'mae': [],
	'precision': [],
	#'recall': [],
	'f1score': []
	}

for i in range(10, 101, 10):
	OBSERVED = '/home/gchatz/Downloads/fhiuzz/tutorials/cmdline_test/observed_{}.json'.format(i)
	EVALUATION = '/home/gchatz/Downloads/fhiuzz/tutorials/cmdline_test/eval_{}.json'.format(i)
	
	fuse_session = fuse_env.get_session('test')

	dataset = fuse_session.ingest(OBSERVED, 'fusion_test')
	#first step prints
	# print("---------Extractions of Dataset -------------------")
	# print(dataset.ent_attr_schema)
	# print(dataset.entity_attributes)
	# print(dataset.entity_attribute_matchers)

	#second step prints
	#dudupe
	fuse_session.train_matchers(PREMATCHED)
	#dedupe clustering
	fuse_session.match_observations()

	tr_clusters = fuse_session.find_true_clusters()

	facts = fuse_session.find_true_facts(persist=False, no_weights=False)

	scores = fuse_session.evaluate(EVALUATION, facts, persist=False)
	
	for key in scores.keys():
		portion_scores[key].append(scores[key])


x = [i for i in range(10, 101, 10)]
plt.figure()
#for key in portion_scores.keys():
#	plt.plot(x, portion_scores[key], label=key)
plt.plot(x,portion_scores['precision'], 'b--',label='precision')
plt.plot(x,portion_scores['recall'], 'g--',label='recall')
plt.plot(x,portion_scores['f1score'],'-o',label='f1score')
plt.legend()
plt.savefig('eval_pre_metrics.png')



