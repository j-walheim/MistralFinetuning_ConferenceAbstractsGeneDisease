

from dagster import load_assets_from_package_module

from . import open_targets_evidence, pubmed_abstracts, training_data, arxiv_abstracts

CS_abstracts = load_assets_from_package_module(package_module=arxiv_abstracts, 
                                              group_name='ComputerScienceAbstracts')

open_targets_evidence_assets = load_assets_from_package_module(package_module=open_targets_evidence, 
                                              group_name='OpenTargetsLiteratureEvidence')

pm_assets = load_assets_from_package_module(package_module=pubmed_abstracts, 
                                              group_name='PubmedAbstracts')

train_assets = load_assets_from_package_module(package_module=training_data, 
                                              group_name='PrepareTrainingData')


