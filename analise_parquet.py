from random import random
import zipfile
import glob
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from sklearn import preprocessing, decomposition
import seaborn as sns
import matplotlib.pyplot as plt

# https://github.com/yzhao062/pyod
# https://pyod.readthedocs.io/en/latest/pyod.html
from pyod.models.suod import SUOD

eventos =[
    datetime(2021,4,8,18,34,0),
    datetime(2021,5,28,11,6,0),
    datetime(2021,5,28,11,26,0),
    datetime(2021,5,28,23,28,0),
    ]

pasta = r'D:\Users\Valerio\OneDrive - Universidade Federal de Itajubá\Repositorio\Eduardo - PMU COPEL\Parquet'

pq = glob.glob(pasta + '\\*.parquet')

for evento in eventos:
    nome_saida = str(evento).replace(':','_')

    data_ini = evento - timedelta(minutes=60)
    data_fim = evento + timedelta(minutes=60)

    df = pd.read_parquet(pq[0])
    mascara = (df['instante'] >= data_ini) & (df['instante'] < data_fim)
    pmu = df[mascara]
    del df

    if len(pmu) == 0: continue

    for i, a in enumerate(pq[1:]):
        nome = os.path.splitext(os.path.basename(a))[0]
        nome_partes = nome.split('-')
        df = pd.read_parquet(a)
        mascara = (df['instante'] >= data_ini) & (df['instante'] < data_fim)
        df = df[mascara]

        if len(df) == 0: 
            del df
            continue

        for col in df.columns[1:]:
            if col in pmu.columns:
                new_col = f'{col}_{i}'
                pmu[new_col] = df[col].copy()
            else:
                pmu[col] = df[col].copy()
        del df

    print('Inicial:',pmu.shape)
    pmu = pmu.dropna(axis=1)
    print('Final:',pmu.shape)
    pmu.to_excel(f'{nome_saida}.xlsx')

    y = pmu['instante'].map(lambda x: 1 if x >= evento - timedelta(minutes=1) and x <= evento + timedelta(minutes=1) else 0 )
    X = pmu[pmu.columns[1:]].values

    Xs = preprocessing.MinMaxScaler().fit_transform(X)
    Xd = decomposition.PCA(n_components=2, random_state=42).fit_transform(Xs)
    #sns.scatterplot(x=Xd[:,0],y=Xd[:,1],hue=y).figure.savefig(f'{nome_saida}.png')
    fig, ax = plt.subplots(figsize=(12, 12))
    colors = {0:'tab:blue', 1:'tab:red'}
    ax.scatter(x=Xd[:,0],y=Xd[:,1],c=y.map(colors))
    plt.savefig(f'{nome_saida}.png')

    del pmu