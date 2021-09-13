
import json
from DataFusion import fuse
OBSERVED = 'C:/Users/Iasonas/Downloads/MSc_DataInt_Code/Data_fusion/tutorials/cmdline_test/observed.json'
PREMATCHED = 'C:/Users/Iasonas/Downloads/MSc_DataInt_Code/Data_fusion/tutorials/cmdline_test/matched.json'
EVALUATION = 'C:/Users/Iasonas/Downloads/MSc_DataInt_Code/Data_fusion/tutorials/cmdline_test/eval.json'


#fuse_env = fuse.Fuse(verbose=True)
fuse_env = fuse.Fuse(verbose=True )
# fuse_env = fuse.Fuse(verbose=True, global_weights=True, matcher_function=lambda a, b: a == b)
fuse_session = fuse_env.get_session('test')

dataset = fuse_session.ingest(OBSERVED, 'fusion_test')
#first step prints
print("---------Extractions of Dataset -------------------")
print(dataset.ent_attr_schema)
print(dataset.entity_attributes)
print(dataset.entity_attribute_matchers)

#second step prints
#dudupe
fuse_session.train_matchers(PREMATCHED)
#dedupe clustering
fuse_session.match_observations()

tr_clusters = fuse_session.find_true_clusters()
for i in tr_clusters:
    print(i)
print("----------------Clusters -----------------")
for c in fuse_session.dataset.cluster_map:
    c1 = fuse_session.dataset.cluster_map[c]
    print ("\n\n New cluster")
    for f in c1.facts:
        print (f.fid, f.eid, f.ent_attr, f.src, f.normalized)

print( "---------------voting--------------------")
for attr in fuse_session.dataset.observed_fact_collections:
    for col in fuse_session.dataset.observed_fact_collections[attr]:
        #Get votes
        votes = col.get_votes()
        print(votes)
        for cluster_id in votes:
            for src in votes[cluster_id]:
               print(cluster_id,src,votes[cluster_id][src])

facts = fuse_session.find_true_facts(persist=False, no_weights=False)

fuse_session.evaluate(EVALUATION, facts, persist=False)
