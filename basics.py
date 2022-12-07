#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkConf, SparkContext

# In[2]:


conf = SparkConf().setAppName("basic")


# In[3]:


sc = SparkContext().getOrCreate(conf=conf)


# In[4]:


text = sc.textFile("Crime_Data_from_2020_to_Present.csv")
text


# In[5]:


data = text.collect()


# In[6]:


print(data[:5])


# In[ ]:
