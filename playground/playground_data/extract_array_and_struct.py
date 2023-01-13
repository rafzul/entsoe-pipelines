from pyspark.sql.types import *
from pyspark.sql import functions as f

def flatten_structs(nested_df):
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        
        parents, df = stack.pop()
        
        array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]
        
        flat_cols = [
            f.col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]
        
        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))
        
    return nested_df.select(columns)

def flatten_array_struct_df(df):
    
    array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]
    
    while len(array_cols) > 0:
        
        for array_col in array_cols:
            
            # cols_to_select = [x for x in df.columns if x != array_col ]
            
            df = df.withColumn(array_col, f.explode(f.col(array_col)))
            
        df = flatten_structs(df)
        
        array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]
    return df

flat_df = flatten_array_struct_df(df)