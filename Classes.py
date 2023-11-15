from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
from datetime import date
from matplotlib import pyplot as plt
import matplotlib as mpl
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType)
from abc import ABC, abstractmethod


class QualityRule(ABC):
    def __init__(self, df, field:str, dtype_expected:str = None, operator:str = None, threshold = None,
                 values:list = None, min_value = None, max_value = None,key:str = None, df2 = None ,
                 field2:str = None, key2:str = None, rule_type:str = None):
        super().__init__()
        self.data = df
        self.field = field
        self.dtype_expected = dtype_expected
        self.operator = operator
        self.threshold = threshold
        self.values = values
        self.min_value = min_value
        self.max_value = max_value
        self.key = key
        self.df2 = df2
        self.field2 = field2
        self.key2 = key2
        self.rule_type = rule_type
        self.population = self._count_population()
        self.errors_df = self._errors_df()
        self.errors = self._count_errors()
        self.compliance = round(((self.population - self.errors)/self.population) * 100,2)
        
    @abstractmethod
    def _count_population():
        pass
    
    @abstractmethod
    def _errors_df():
        pass
    
    @abstractmethod
    def _count_errors():
        pass
    
    def save(self, container:list):
        self.container = container
        self.container.append(
            {"Field": self.field,
             "Secondary_Field": self.field2,
             "Object": [x for x in globals() if globals()[x] is self.data][0],
             "Rule_Type" : self.rule_type,
             "Population": self.population,
             "Errors":self.errors,
             "Compliance": self.compliance}
)

class R_1_1(QualityRule):    
    
    def __init__(self, df, field, dtype_expected):
        super().__init__(df, field, dtype_expected, rule_type = '1.1')
        
    def _count_population(self):
        return len(self.data.select(self.field).dtypes[0][1].split())
    
    def _errors_df(self):
        return '-'
        
    def _count_errors(self):
        if self.data.select(self.field).dtypes[0][1] == self.dtype_expected:
            return 0
        else:
            return 1

class R_1_2(QualityRule):

    def __init__(self, df, field):
        super().__init__(df, field, rule_type = '1.2')
        
    def _count_population(self):
        return self.data.count()
    
    def _errors_df(self):
        return self.data.filter(F.col(self.field).isNull())
    
    def _count_errors(self):
        return self.errors_df.count()

class R_1_3_D(QualityRule):    
    
    def __init__(self, df, field, values):
        super().__init__(df = df, field = field, values = values, rule_type = '1.3_D')
        
    def _count_population(self):
        return self.data.count()
    
    def _errors_df(self):
        self.data.filter(F.col(self.field).isin(self.values))
        
    def _count_errors(self):
        return self.errors_df.count() 

class R_1_3_C(QualityRule):    
    
    def __init__(self, df, field, operator, threshold):
        super().__init__(df = df, field = field, operator = operator, threshold = threshold, rule_type = '1.3_C')
        
    def _count_population(self):
        return self.data.count()
    
    def _errors_df(self):
        
        if self.operator == '<=':
            return self.data.filter(F.col(self.field) <= self.threshold)
        elif self.operator == '<':
            return self.data.filter(F.col(self.field) < self.threshold)
        elif self.operator == '>':
            return self.data.filter(F.col(self.field) > self.threshold)
        elif self.operator == '>=':
            return self.data.filter(F.col(self.field) >= self.threshold)
        elif self.operator == '==':
            return self.data.filter(F.col(self.field) == self.threshold)
        elif self.operator == '!=':
            return self.data.filter(F.col(self.field) != self.threshold)
        
    def _count_errors(self):
        return self.errors_df.count()   

class R_1_4(QualityRule):    
    
    def __init__(self, df, field:str, min_value, max_value):
        super().__init__(df = df, field = field, min_value = min_value, max_value = max_value, rule_type = '1.4')
        
    def _count_population(self):
        return self.data.count()
    
    def _errors_df(self):
        return self.data.filter((F.col(self.field) < self.min_value) | (F.col(self.field) > self.max_value))
        
    def _count_errors(self):
        return self.errors_df.count()

class R_1_5(QualityRule):    
    
    def __init__(self, df, field:str, values):
        super().__init__(df = df, field = field, values = values, rule_type = '1.5')
        
    def _count_population(self):
        return self.data.count()
    
    def _errors_df(self):
        return self.data.filter(~F.col(self.field).isin(self.values))
        
    def _count_errors(self):
        return self.errors_df.count()

class R_1_6(QualityRule):    
    
    def __init__(self, df, field:str):
        super().__init__(df = df, field = field, rule_type = '1.6')
        
    def _count_population(self):
        return self.data.count()
    
    def _errors_df(self):
        return self.data.withColumn('blanks', F.length(F.regexp_replace(self.field, '[^ ]+', ''))).filter(F.col('blanks')!= 0)
        
    def _count_errors(self):
        return self.errors_df.count()

class R_1_7(QualityRule):
    
    def __init__(self, df, field):
        super().__init__(df, field, rule_type = '1.7')
        
    def _count_population(self):
        return self.data.count()
    
    def _errors_df(self):
        return self.data.select(self.field).groupBy(self.field).count().withColumn('errores',F.col('count')-1)
    
    def _count_errors(self):
        return self.errors_df.select(F.sum('errores')).collect()[0][0]

schema = StructType([ \
    StructField("Field",StringType(),True), \
    StructField("Secondary_Field",StringType(),True), \
    StructField("Object",StringType(),True), \
    StructField("Rule_Type", StringType(), True), \
    StructField("Population", IntegerType(), True), \
    StructField("Errors", IntegerType(), True), \
    StructField("Compliance", DoubleType(), True) \
  ])

def finish_report(container: list):
        reporte_df = (spark.createDataFrame(container, schema)
                      #.withColumn('ID', F.monotonically_increasing_id())
                      #.withColumn('ID_order', F.row_number().over(Window.orderBy('ID')))
                      #.withColumn('OKs', F.col('Population')-F.col('Errors'))
                      #.select('ID_order', 'Field','Secondary_Field','Object', 'Rule_Type', 'Population', 'OKs', 'Errors', 'Compliance')
                      )
        return reporte_df

def plot_report(reporte_df: pd.DataFrame):
    
    fig, ax = plt.subplots(figsize=(15,8), facecolor=(.94, .94, .94))

    reglas = reporte_df.select('ID_order').rdd.flatMap(lambda x:x).collect()
    cumplimiento = reporte_df.select('Compliance').rdd.flatMap(lambda x:x).collect()

    cols = ['red' if x < 90 else 'green' if x > 90 else 'yellow' for x in cumplimiento]
    bars = ax.barh(reglas,cumplimiento, color = cols, align='center')
    plt.axvline(x = 90, color = 'red', ls='--')
    plt.axvline(x = 95, color = 'yellow', ls='--')
    
    ax.set_facecolor('#eafff5')
    #etiqueta de barras
    ax.bar_label(bars, fmt= '{:,.2f}%', label_type ='center', color = 'white')
    #Formato de eje y
    ax.xaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}%'))
    #Etiquetas de ejes
    ax.set(ylabel='ID Rule', xlabel = 'Compliance')
    #Titulo
    title = plt.title('Quality Rules Results',fontsize=18,pad=20)
    title.set_position([.12, 1])
    plt.show()

# %%
def compliance(df: pd.DataFrame , by = 'Total'): 
    
    print(' Cantidad de Reglas: ', df.count(),'\n',
          'Cantidad de objetos: ', df.select('Object').distinct().count(),'\n',
          'Cantidad de campos: ', df.select('Field').distinct().count(),'\n',
          'Reglas por campo: ',round(df.count() / df.select('Field').distinct().count(),2),'\n',
          'Reglas por objeto: ',round(df.count() / df.select('Object').distinct().count()),2,'\n')
      
    if by == 'Total':
        return print('% de Cumplimiento Total: ', df.select(F.round(F.avg('Compliance'),2)).collect()[0][0])
    
    elif by == 'Objects':
        obj_cump = df.select('Object','Compliance').groupBy('Object').agg(F.round(F.avg('Compliance'),2).alias('mean'))
        objects = obj_cump.select('Object').rdd.flatMap(lambda x:x).collect()
        cump = obj_cump.select('mean').rdd.flatMap(lambda x:x).collect()
        for i,j in zip(objects, cump):
            print('% de Cumplimiento -', i,':', j)
            
    elif by == 'Fields':
        fields_cump = df.select('Field','Compliance').groupBy('Field').agg(F.round(F.avg('Compliance'),2).alias('mean'))
        fields = fields_cump.select('Field').rdd.flatMap(lambda x:x).collect()
        cump = fields_cump.select('mean').rdd.flatMap(lambda x:x).collect()
        for i,j in zip(fields, cump):
            print('% de Cumplimiento -', i,':', j)


