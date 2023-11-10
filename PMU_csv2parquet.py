from random import random
import zipfile
import glob
import pandas as pd
import numpy as np
import os
from datetime import datetime

# No dia 08/04/2021, às 18h34, ocorreu o desligamento automático das LT 230 kV Jurupari/Laranjal C1 e C2, bem como das UHE Ferreira Gomes, Cachoeira Caldeirão, Coaracy Nunes e Santo Antônio do Jari, totalizando uma perda de 800 MW de geração, o que levou ao desligamento total do sistema Amapá.
# No dia 28/05/2021, às 11h06, ocorreu o desligamento automático do polo 1 do Elo CC 800 kV Xingu / Estreito (BMTE) com 1.996 MW, sendo essa potência assumida pelo polos remanescentes, sem maiores consequências para o SIN.
# Às 11h26, ocorreu o desligamento automático do polo 2 do Elo CC 800 kV Xingu / Estreito (BMTE) e de 7 unidades geradoras da UHE Belo Monte (Norte Energia S.A.), as quais estavam gerando no momento um montante de aproximadamente 4.050 MW.
# No dia 28/05/2021, às 23h28, ocorreu o desligamento automático das LT 230 kV Campos Novos / Videira C1 e C2 (Evoltz VI).

eventos =[
    datetime(2021,4,8,18,34,0),
    datetime(2021,5,28,11,6,0),
    datetime(2021,5,28,11,26,0),
    datetime(2021,5,28,23,28,0),
    ]

pastas = [
    r'D:\evento 1 - 08-04-2021',
    r'D:\evento 2 - 28-05-2021',
    ]

chunksize = 3600 
dispositivo_anterior = ''
taxas=[]
cwd = os.getcwd()

for p in pastas:
    subpasta = p.split('\\')[-1]
    print('PASTA:',subpasta)
    gp = []
    for a in glob.glob(p + '\\*.zip'):
        nome = os.path.splitext(os.path.basename(a))[0]
        nome_partes = nome.split('-')
        gp.append({
            'pmu_id':nome_partes[1],
            'dispositivo':'-'.join(nome_partes[2:-2]),            
            'data_ini':datetime.strptime(nome_partes[-2],'%Y%m%d%H%M%S'), #2021-04-08-06-00-00
            'data_fim':datetime.strptime(nome_partes[-1],'%Y%m%d%H%M%S'),
            'fullname':a,
            })
    grupos = pd.DataFrame(gp)
    del gp

    for pmu, f in grupos.groupby('pmu_id'):
        
        nome_salvar = f'{subpasta} - {pmu}.parquet'

        if os.path.exists(nome_salvar):
            continue

        medidas = []

        for i, r in f.sort_values('data_ini').iterrows():
            nome_zip = r['fullname']

            with zipfile.ZipFile(nome_zip,'r') as z:
                for ex in z.namelist():
                    print(' - Extraindo arquivo:',ex)
                    csv = z.extract(ex, cwd)

                    for df in pd.read_csv(csv,skiprows=1,chunksize=chunksize):
                        instante = df[df.columns[0]].values[0] + ' ' +  df[df.columns[1]].values[0]
                        instante = datetime.strptime(instante,'%d/%m/%y %H:%M:%S.%f')

                        dn = {'instante':instante}
                        if 'df/dt' in df.columns: dn['rocof'] = df['df/dt'].max()-df['df/dt'].min()
                        if 'Frequency' in df.columns: dn['freq'] = df['Frequency'].max()-df['Frequency'].min()

                        colunas = pd.DataFrame(
                            [{'coluna':col, 'dispositivo':col.split(' ')[1], 'medida':col.split(' ')[0]} for col in df.columns if ' Magnitude' in col]
                        )

                        for grupo in colunas.groupby('dispositivo'):
                            variacao = []
                            for id, row in grupo[1].iterrows():
                                min = df[row['coluna']].min()
                                max = df[row['coluna']].max()
                                variacao.append(max - min)
                            dn[grupo[0]] = np.max(variacao)

                        medidas.append(dn)
                    
                    print(' - Removendo arquivo;')
                    os.remove(csv)
        
        mdf = pd.DataFrame(medidas)
        del medidas
        mdf.to_parquet(nome_salvar)
        del mdf

print('-Terminado!')
