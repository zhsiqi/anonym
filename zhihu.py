#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 19:08:08 2021

@author: zhangsiqi
"""

from zhihu_oauth import ZhihuClient
from zhihu_oauth.helpers import shield
from zhihu_oauth.helpers import ts2str
import time
import sqlite3 as sqlite
import os
from pathlib import Path

p = Path('知乎爬虫')
if p.exists():
    os.chdir(p)
else:
    p.mkdir()
    os.chdir(p)

# 登录知乎
client = ZhihuClient()
client.login_in_terminal()

# 创建sql数据库
conn= sqlite.connect('zhihu.sqlite')
c = conn.cursor()

"""
=====话题======
话题id：topic.id
话题名称：topic.name
话题简介：topic.introduction
话题关注人数：topic.follower_count
话题问题数：topic.question_count
话题精华回答数：topic.best_answer_count
=====问题======
回答所在的问题的id：answer.question.id
问题的标题：quest.title
问题的关注人数：quest.follower_count
问题的创建时间：quest.created_time
问题的编辑时间：quest.updated_time
=====回答======
答主昵称与id：answer.author.name, answer.author.id
回答的评论权限：answer.comment_permission
回答的评论数：answer.comment_count
回答的点赞数：answer.voteup_count
回答的感谢数：answer.thanks_count
回答的创建时间： answer.created_time
问答的编辑时间： answer.updated_time
回答内容: answer.content
回答的id：answer.id
=====评论======
评论的id：comment.id
评论者的昵称：comment.author.name
评论者的id：comment.author.id
评论的创建时间：comment.created_time
评论内容：comment.content
"""

# 创建表单，保存话题
c.execute('''CREATE TABLE IF NOT EXISTS topic 
          (topic_id int, topic_title text, introduction text, 
           follower_count int, question_count int, best_answer_count int)''')
# 创建表单，保存话题下面精华回答所在的问题
c.execute('''CREATE TABLE IF NOT EXISTS question 
          (topic_id int, question_id unique, question_title text, 
           follower_count int, created_time text, updated_time)''')
# 创建表单，保存精华问题的回答
c.execute('''CREATE TABLE IF NOT EXISTS answer 
          (question_id int, answer_id unique, author text, author_id text,  
           comment_permission text, comment_count int, voteup_count int, thanks_count int,
           created_time text, updated_time, answer_content text)''')
# 创建表单，保存回答下的评论
c.execute('''CREATE TABLE if NOT EXISTS comment 
          (question_id int, answer_id int, comment_id unique, author text, author_id text, 
           created_time text, content text)''')


# 将topic相关数据写入表单，话题“匿名”的id是19576616
topic_id = 19576616
topic = client.topic(topic_id)
values = (topic.id, topic.name, topic.introduction, 
          topic.follower_count, topic.question_count, topic.best_answer_count)
c.execute(''' insert into topic
              (topic_id, topic_title, introduction, 
               follower_count, question_count, best_answer_count)
              values(?, ?, ?, ?, ?, ?) ''', values)
conn.commit()

# 将精华回答所在的问题数据写入表单，取前5个问题
qid_list=[]
for m, answer in enumerate(shield(topic.best_answers, start_at=0, action="PASS"), start = 1):
    if answer.question.id not in qid_list:
        qid_list.append(answer.question.id)
        quest = client.question(answer.question.id)
        values = (topic_id, answer.question.id, quest.title, 
              quest.follower_count, ts2str(quest.created_time), ts2str(quest.updated_time))
        c.execute(''' insert into question
                  (topic_id, question_id, question_title, 
                   follower_count, created_time, updated_time)
                  values(?, ?, ?, ?, ?, ?) ''',  values)
        print("{} questions have been downloaded \n Sleeping for 3 seconds ... ".format(len(qid_list)))
        time.sleep(3)
    if len(qid_list) > 4: 
        break
print('前5个问题的id', qid_list)    
conn.commit()



for qid in qid_list:
    quest = client.question(qid)
    # 将前5个问题的回答数据写入表单，每个问题取前10个回答
    for i, answer in enumerate(shield(quest.answers, start_at=0, action="PASS"), start = 1):
        values = (qid, answer.id, answer.author.name, answer.author.id, 
                  answer.comment_permission, answer.comment_count, answer.voteup_count, 
                  answer.thanks_count, ts2str(answer.created_time), 
                  ts2str(answer.updated_time), answer.content)
        c.execute(''' insert into answer
                  (question_id, answer_id, author, author_id, 
                  comment_permission, comment_count, voteup_count, thanks_count,
                  created_time, updated_time, answer_content)
                  values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ''',  values)
        print("{} answers of question {} have been downloaded\n Sleeping for 3 seconds ... ".
              format(i, qid_list.index(qid)+1))
        time.sleep(3) 
        conn.commit() 
        
        # 将评论数据写入表单，每个回答取前2个评论
        comments = answer.comments
        for j, comment in enumerate(shield(comments, action="PASS"), 1):
            values = (answer.question.id, answer.id, comment.id, 
                      comment.author.name, comment.author.id,
                      ts2str(comment.created_time), comment.content)
            c.execute(''' insert or ignore into comment
                      (question_id, answer_id, comment_id, author, author_id, 
                       created_time, content) values(?, ?, ?, ?, ?, ?, ?) ''',  values)
            print("{} comments of anwser {} of question {} have been downloaded\n Sleeping for 3 seconds ... ". 
                  format(j, i, qid_list.index(qid)+1))
            time.sleep(3)
            conn.commit() 
            if j > 1:
                break
        if i > 9:
            break

conn.close()
