# SPOKE
Welcome to the SPOKE project!

# Development Notes

## Requirements
Requirements are defined from the `environments.yml` file. Development is done
primarily using conda environments defined by this file. Instructions for
creating and updating a conda enviroment from an environments file can be found
at the [conda enviroments reference
page](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html).

## Datasets

Datasets are managed using [DVC](https://dvc.org/). See the documentation for
how to install it in your specific setup, but if you use a Mac and have homebrew
then you can just run:

```
brew install dvc
```

Once you have `dvc`, run the following commands:

```
# This installs Git hooks to make sure that all of the necessary commands are
# run automatically going forward.
dvc install
# This checks out the dvc cache and pulls the actual data from the remote to
# your local storage
dvc pull
```