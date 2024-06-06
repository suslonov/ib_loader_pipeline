export PATH="/opt/conda/miniconda3/bin:$PATH"

AWS_ACCESS_KEY=$($HOME/.s3keys AWS_ACCESS_KEY)
AWS_SECRET_ACCESS_KEY=$($HOME/.s3keys AWS_SECRET_ACCESS_KEY)

export AWS_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY

python $1/current_data_slice.py $2/ib_current_slice
