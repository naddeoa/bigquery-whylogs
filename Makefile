NAME=test
METHOD=
JOB_NAME=method-$(METHOD)--$(NAME)
BUCKET=gs://$(USER)_beam/$(JOB_NAME)
TMP_BUCKET=$(BUCKET)/tmp
REGION=us-west1 # us-west2, us-central1

.PHONY: run 2 default

default:help

1:METHOD=1
1:run-container ## Attempts to use GroupIntoBatches to feed lists of rows into the combiner.

1-control:METHOD=1-control
1-control:run-container ## Same as 1 but it doesn't use whylogs at all.

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



help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.+) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'
