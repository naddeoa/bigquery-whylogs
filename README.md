The makefile has a few canned targets that you can execute. I hard coded stuff in the source code as Method 1 - 3 and made make targets for them.

```bash
make help

# Examples
# Execute method 1 in the source. NAME is part of the job name/gcs folder name. REGION defaults to us-west1.
make 1 NAME=typecehck-test REGION=us-west2
```

This is what its doing.

```python
python ./test.py --output gs://anthony_beam/method-1--typecheck-test-should-fail-asap \
   --temp_location gs://anthony_beam/method-1--typecheck-test-should-fail-asap/tmp \
   --job_name method-1--typecheck-test-should-fail-asap \
   --runner DataflowRunner \
   --project whylogs-xxxxx\
   --region us-west1 \
   --requirements_file=requirements.txt \
   --num_workers 10 \
   --method 1 # custom option from me to pick a pipeline to run
```
