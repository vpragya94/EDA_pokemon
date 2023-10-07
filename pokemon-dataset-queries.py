#!/usr/bin/env python
# coding: utf-8

# In[6]:


import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# In[7]:


spark = (
    SparkSession
    .builder
    .getOrCreate()
)
spark


# In[11]:


sc = spark.sparkContext


# In[12]:


pok = sc.textFile("/home/nineleaps/Downloads/Pokemon_Dataset.csv")


# In[15]:


pok.collect()


# In[16]:


pok.first()


# In[18]:


pok.collect()


# In[19]:


pok.count()


# In[20]:


pok = spark.read.csv("/home/nineleaps/Downloads/Pokemon_Dataset.csv", header=True, inferSchema=True)


# In[30]:


pok.show()


# In[25]:


avghp = spark.sql("select avg(HP) as avgg from pok ").show()


# In[26]:


pok.createOrReplaceTempView("pokemon")


# In[28]:


avghp = spark.sql("select avg(HP) as avgg from pokemon ").show()


# In[31]:


tenhp = spark.sql("select * from pokemon order by HP desc limit 10").show()


# In[38]:


ten = spark.sql("""select * from (select * , dense_rank() over(order by HP desc) as dr from pokemon ) as sub 
                where dr<=10""").show()


# In[39]:


attk = spark.sql("select * from pokemon order by attack desc limit 10").show()


# In[42]:


attkk = spark.sql("""select * from (select * , rank() over(order by attack desc) as rnk from pokemon) as subq  
                  where rnk <=10""").show()


# In[43]:


defense = spark.sql("""select * from (select * , rank() over(order by defense desc) as rnk from pokemon) as subq  
                  where rnk <=10""").show()


# In[46]:


totall = spark.sql("""select * from (select * , rank() over(order by total desc) as rnk from pokemon) as subq  
                  where rnk <=10""").show()


# In[47]:


dc = spark.sql("""select * , abs(attack - `sp. atk`) as dc from pokemon order by dc desc limit 10""").show()


# In[48]:


drc = spark.sql("""select * , abs(defense - `sp. def`) as drc from pokemon order by drc desc limit 10""").show()


# In[49]:


speed = spark.sql("select * from pokemon order by speed desc limit 10").show()


# In[50]:


speed = spark.sql("""select * from (select * , rank() over(order by speed desc) as rnk from pokemon) as subq  
                  where rnk <=10""").show()


# In[52]:


pok.count()


# In[ ]:




