#!/bin/bash

# Submit a get_features job to the execution grid.  This will create
# a per-job directory under $base_dir/jobs/.  Within that directory
# will be created a number of files:
#
# job.bash is a generated job control file for jsub
#
# jsub.out, jsub.err or the stdout/stderr of the jsub command.  These
# are generally empty.
#
# job.out, job.err are the stdout/stderr of the grid job itself.  The
# latter is generally empty; if it's not, you need to investigate.
#
# A symlink will be created from $base_dir/jobs/last to the per-job
# directory.  This is a convenience for interactive use, but should
# not be counted on for production purposes..  No attempt is made to
# avoid colissions.  If you run multiple jobs in parallel, the per-job
# directories will be fine, but the symlink will point to whichever
# one got there last and stepped on the others.
#
# In addition, logging will be to a log file shared by all
# get-features jobs ($base_dir/logs/get-features.log).  There's no
# need to have a shared log file.  I just find it convenient because I
# can keep "tail -f" running on it all the time in a window to keep an
# eye on things.


# You'll likely want to customize this.
base_dir=$HOME/sock-classifier

# If you're OK with the default directory layout, you
# shouldn't need to touch anything after here.

if [ $# -ne "1" ]; then
    echo "Usage: submit-get-features.bash archive-dir"
    exit 1
fi
archive_dir=$1

job_name=get_features.`date +%Y-%m-%d-%H-%M-%S`
job_dir=$base_dir/jobs/$job_name
mkdir $job_dir
last=$base_dir/jobs/last
rm -f $last
ln -s $job_dir $last
echo $job_dir

cat > $job_dir/job.bash << EOF
#!/bin/bash

source $base_dir/env/bin/activate

$base_dir/src/utils/get_features.py \\
  --archive-dir $base_dir/data/$archive_dir \\
  --log=$base_dir/logs/get_features.log

EOF
chmod +x $job_dir/job.bash

jsub \
  -N $job_name \
  -o $job_dir/job.out \
  -e $job_dir/job.err \
  $job_dir/job.bash \
  > $job_dir/jsub.out \
  2> $job_dir/jsub.err
