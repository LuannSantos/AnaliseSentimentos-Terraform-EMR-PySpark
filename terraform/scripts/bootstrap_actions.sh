# install conda
wget --quiet https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh \
    && /bin/bash ~/miniconda.sh -b -p $HOME/conda

echo -e '\nexport PATH=$HOME/conda/bin:$PATH' >> $HOME/.bashrc && source $HOME/.bashrc

pip install --upgrade pip
pip install findspark
pip install pendulum
pip install boto3
pip install python-dotenv

mkdir $HOME/python
mkdir $HOME/logs
