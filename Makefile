NAME=test
METHOD=
JOB_NAME=method-$(METHOD)--$(NAME)
BUCKET=gs://$(USER)_beam/$(JOB_NAME)
TMP_BUCKET=$(BUCKET)/tmp
REGION=us-west1 # us-west2, us-central1

.PHONY: run 2 default run-local run-container template

default:help

1:METHOD=1
1:run-container ## Attempts to use GroupIntoBatches to feed lists of rows into the combiner.

1-control:METHOD=1-control
1-control:run-container ## Same as 1 but it doesn't use whylogs at all.

1-crypto:METHOD=1-crypto
1-crypto:run-container ## Same as 1 but with a larger crypto dataset, grouping daily.

2:METHOD=2
2:run-container ## Does profiling element-wise in a map step and reduces into views.

3:METHOD=3
3:run-container ## Simple map reduce, but very slow

noop:METHOD=noop
noop:run-container ## Does the same thing 1 does but the reducer returns constants.

counts:METHOD=counts
counts:run-container ## Job that computes the count of rows per day timestamp

window-1:METHOD=window-1
window-1:run-container ## Run with daily windowing instead of creating day keys manually

window-batched:METHOD=window-batched
window-batched:run-container ## Run with daily windowing instead of creating day keys manually

less-shuffle:METHOD=less-shuffle
less-shuffle:run-container

less-shuffle-multiple-profiles:METHOD=less-shuffle-multiple-profiles
less-shuffle-multiple-profiles:run-container

run:
	python ./test.py \
	   --output $(BUCKET) \
	   --temp_location $(TMP_BUCKET) \
	   --job_name $(JOB_NAME) \
	   --runner DataflowRunner \
	   --project whylogs-359820 \
	   --region $(REGION) \
	   --requirements_file=requirements.txt \
	   --method $(METHOD)
	   
run-local:
	python ./test.py \
	   --output ./$(NAME)_output \
	   --temp_location $(TMP_BUCKET) \
	   --project whylogs-359820 \
	   --requirements_file=requirements.txt \
	   --method $(METHOD)

run-container:
	python ./test.py \
	   --output $(BUCKET) \
	   --temp_location $(TMP_BUCKET) \
	   --job_name $(JOB_NAME) \
	   --runner DataflowRunner \
	   --project whylogs-359820 \
	   --region $(REGION) \
	   --experiment=use_runner_v2 \
	   --sdk_container_image=naddeoa/whylogs-dataflow-dependencies:no-analytics \
	   --method $(METHOD) 
#    --worker_machine_type=m1-ultramem-40 --disk_size_gb=500

# Machine types: https://cloud.google.com/compute/docs/memory-optimized-machines


help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.+) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'



template:
	python -m ai.whylabs.profile_single_period \
	  --runner DataflowRunner \
	  --project whylogs-359820 \
	  --staging_location gs://anthony_beam/staging \
	  --temp_location gs://anthony_beam/temp \
	  --template_location gs://anthony_beam/templates/profile_single_period \
	  --region us-west1
