import pandas as pd
import numpy as np
import os
from datetime import date
from global_variables import *


class StockManager:
    # init ---------------------------------------------------------------------
    def __init__(self) -> None:
        self.__stock = self.__gather_stock()
    

    def __gather_stock(self) -> pd.DataFrame:
        df_completo_stock = pd.DataFrame()
        for archivo in os.listdir(SAP_PATH):
            try:
                if archivo.startswith('Stock'):
                    dia = pd.Timestamp(archivo.split('_')[-1].split('.')[0]) \
                        - pd.Timedelta(1, 'D') 
                    df_stock = pd.read_csv(
                        SAP_PATH + archivo, 
                        encoding='utf-16', sep=';'
                    ).rename({'MATNR': 'Material', 'STOCK': 'Stock'}, axis=1)
                    df_stock['Fecha'] = dia
                    df_stock.set_index('Fecha', inplace=True)

                    df_completo_stock = pd.concat([df_completo_stock, df_stock])

                elif archivo.startswith('ZMM_PBL_ENV_ART'):
                    dia = pd.Timestamp(archivo.split('.')[0].split('-')[-2]) \
                        - pd.Timedelta(1, 'D')
                    df_stock = pd.read_csv(
                        SAP_PATH + archivo, 
                        encoding='latin1', sep=';'
                    ).rename({'CODART': 'Material', 'STOCK': 'Stock'}, axis=1)
                    df_stock.head()
                    df_stock['Fecha'] = dia
                    df_stock.set_index('Fecha', inplace=True)

                    df_completo_stock = pd.concat([df_completo_stock, df_stock])
            except Exception as e:
                raise ValueError('Archivo ' + archivo + '  incorrecto en stock')
        return df_completo_stock.drop_duplicates()


    # Dias stock ---------------------------------------------------------------
    def estimate_stock_lifetime( 
            # self, fecha:pd.Timestamp, materiales:int|list=-1) -> pd.DataFrame:
            self, fecha:pd.Timestamp, materiales:int=-1) -> pd.DataFrame:
        stock_in = self.stockByDate(fecha)
        if type(materiales) is int:
            materiales = [materiales]
        resultados = pd.DataFrame({'Material': [], 'Dias Stock': []})
        for archivo in os.listdir(PIPELINE_PATH):
            if archivo.endswith('_demanda.parquet'):
                material = int(archivo.split('/')[-1].split('_')[0])
                # Si materiales es -1 es el valor por defecto y se analizan todos
                if (materiales == [-1]) | (material in materiales):
                    
                    dias_stock = 0
                    stock = stock_in.loc[material, 'Stock'].values[0]
                    fecha_ini = stock_in.loc[material].index.values[0]
                    fecha_end = (fecha_ini - pd.Timedelta(6, 'D'))
                    fecha_rango = pd.date_range(fecha_ini, fecha_end)
                    try:
                        df_demanda = pd.read_parquet(PIPELINE_PATH + archivo)
                        particion = df_demanda.loc[fecha_rango]
                        if particion['Demanda'].sum() == 0:
                            fecha_end = max(
                                df_demanda[df_demanda['Demanda'] > 0].index
                            )
                            new_date_range = pd.date_range(fecha_end, fecha_ini)
                            particion = df_demanda.loc[new_date_range]
                        if (len(particion) != len(df_demanda)) | (stock == 0):
                            particion = particion['Demanda']
                            fin_puntero = len(particion) - 1
                            puntero = 0
                            while stock > 0:
                                stock -= particion.values[puntero]
                                if puntero == fin_puntero:
                                    puntero = 0
                                else:
                                    puntero += 1
                                dias_stock += 1
                            new_row = pd.DataFrame({'Material': [material], 
                                                    'Dias Stock': [dias_stock]})
                            resultados = pd.concat([resultados, new_row])
                        else:
                            # Si el material tiene demanda 0 en 750 días
                            new_row = pd.DataFrame({'Material': [material], 
                                                    'Dias Stock': [1000]})
                            resultados = pd.concat([resultados, new_row])
                    except:
                        pass
        resultados = resultados.astype(int)
        return resultados


    def stockByDate(self, fecha:pd.Timestamp) -> pd.DataFrame:

        stock = self.__stock
        stock = stock.reset_index().pivot(
            index='Fecha', columns='Material', values='Stock'
        )
        stock.fillna(0, inplace=True)
        stock = stock.loc[[fecha]]
        value_cols = stock.columns
        stock = pd.melt(
            stock.reset_index(), id_vars=['Fecha'], 
            value_vars=value_cols, value_name='Stock'
        )
        stock.set_index(['Material', 'Fecha'], inplace=True)
        stock = stock.astype(int)
        return stock


    # Generar informe ----------------------------------------------------------
    def generateReport(self, fecha:pd.Timestamp) -> None:
        dias_restantes = self.estimate_stock_lifetime(fecha)
        dias_restantes.set_index('Material', inplace=True)

        stock_inicial = self.stockByDate(fecha).reset_index() \
            .set_index('Material').drop('Fecha', axis=1)

        dias_restantes_join = dias_restantes.join(stock_inicial, how='left') \
            .fillna(0).astype(int)
        
        str_fecha = ''.join(str(fecha).split(' ')[0].split('-'))
        archivo = 'MARA-DATA-VMD_' + str_fecha +'-0001.csv'
        vmd = pd.read_csv(SAP_PATH + archivo, sep=';')
        vmd = vmd.loc[vmd['MATNR'].str.isdigit()]
        vmd.rename({'MATNR': 'Material', 'ZZVMD': 'VMD'}, axis=1, inplace=True)
        vmd = vmd.astype({'Material': int, 'VMD': float}) 
        vmd.set_index('Material', inplace=True)

        dias_restantes_join = dias_restantes_join.join(vmd, how='left').fillna(0)
        dias_restantes_join.to_excel(
            PIPELINE_PATH + '/Stock/Comprobacion_stock_' + str_fecha + '.xlsx'
        )
        return dias_restantes_join


    # Setters & Getters --------------------------------------------------------
    @property
    def getStock(self):
        return self.__stock
    

if __name__ == '__main__':
    # fecha = pd.Timestamp(date.today()) - pd.Timedelta(1, 'D')
    fecha = pd.Timestamp('2023-05-31')
    sm = StockManager()
    stock_lt, vmd = sm.generateReport(fecha).loc[603035, ['Dias Stock', 'VMD']]
    print(stock_lt, vmd)
