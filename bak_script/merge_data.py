# coding = utf-8
"""
-*- coding: utf-8 -*-

@Author  : houcg
@Time    : 2024/7/2 17:51
"""
import numpy as np
import pandas
import os
import csv

# pmbmed_data = pandas.read_csv('./newest/pubmed_data_new(all).csv')
pmbmed_data = pandas.read_csv('./newest/pubmed_merge.csv')
ncbi_data = pandas.read_csv('./newest/ncbi_data(all).csv', encoding='utf8')
ncbi_data["PubmedID"] = ncbi_data["PubmedID"].astype(str)


def split_data():
    print(ncbi_data.info())
    # 假设您的DataFrame对象名为df，包含一个名为PubmedID的列
    # 创建一个新的DataFrame对象，用于存储拆分后的数据
    pmid_new_df = []
    print(len(ncbi_data))
    # 遍历原始DataFrame的每一行
    for index, row in ncbi_data.iterrows():

        if pandas.isna(row['PubmedID']):

            pmid_new_df.append(row.copy())
        else:

            print(row['PubmedID'])
            pubmed_ids = row['PubmedID'].split('，')  # 将PubmedID按逗号拆分成多个部分

            # 对于每个拆分后的PubmedID，创建一个新的行并复制其他列的值
            for pubmed_id in pubmed_ids:
                new_row = row.copy()
                new_row['PubmedID'] = pubmed_id.strip()  # 去除拆分后的PubmedID字符串中的空格
                pmid_new_df.append(new_row)

    # 打印拆分后的DataFrame
    # print(new_df)
    print(len(pmid_new_df))

    pmid_split_df = pandas.DataFrame(pmid_new_df)
    journal_new_df = []

    # 遍历原始DataFrame的每一行
    for index, row in pmid_split_df.iterrows():
        if pandas.isna(row['Journal']):
            journal_new_df.append(row.copy())
        else:
            journal_list = row['Journal'].split('，')  # 将PubmedID按逗号拆分成多个部分

            # if len(journal_list) > 1:
            #     print(journal_list)
            # 对于每个拆分后的Journal，创建一个新的行并复制其他列的值
            for journal in journal_list:
                new_row = row.copy()
                new_row['Journal'] = journal.strip()  # 去除拆分后的PubmedID字符串中的空格
                journal_new_df.append(new_row)
    print(len(journal_new_df))

    journal_split_df = pandas.DataFrame(journal_new_df)
    journal_split_df.to_csv('./newest/ncbi_data_split(all).csv', index=False, index_label=False, encoding="utf-8")


def join_data():
    split_df = pandas.read_csv('./newest/ncbi_data_split(all).csv')
    split_df["PubmedID"] = split_df["PubmedID"].astype(object)

    # print(split_df.columns.to_list())
    # print(split_df.info())
    cols = ['Title', 'Accession', 'Organism', 'Summary', 'Overall_design', 'PubmedID', 'Journal']

    split_df.columns = cols
    print(split_df.info())
    merged_df = pandas.merge(pmbmed_data, split_df, on='PubmedID', how='outer')
    print(merged_df.info())

    # merged_df.to_csv("./newest/ncbi_data_split(all).csv" , index=False, index_label=False, encoding="utf-8")
    merged_df = merged_df.rename(columns={'Title_x': 'title_pub', 'Title_y': 'title_geo', 'Journal_x': 'journal_pub',
                                          'Journal_y': 'journal_geo'})
    print(merged_df.info())

    # merged_df.to_csv("./newest/join_result(all).csv", index=False, index_label=False, encoding="utf-8")

    merged_df = pandas.concat([merged_df, ncbi_data], axis=0, ignore_index=True)

    # 对没有Accession列且不为空的行进行by PubMedID去重
    df_no_accession = merged_df[merged_df['Accession'].isnull()].drop_duplicates(subset=['PubmedID'])

    # 对有Accession列不为空的行进行by Accession去重
    df_with_accession = merged_df[merged_df['Accession'].notnull()].drop_duplicates(subset=['Accession'])

    # 合并两个数据框
    final_df = pandas.concat([df_no_accession, df_with_accession])

    # 重置索引
    final_df.reset_index(drop=True, inplace=True)

    final_df.to_csv("./newest/join_result(all).csv", index=False, index_label=False, encoding="utf-8")

    # 筛选包含'Homo' 或 'Mus'的行
    # filtered_df = final_df[final_df['Organism'].str.contains('Homo|Mus', na=True)]
    
    filtered_df=final_df
    total_count=len(filtered_df)
    print("总条目数：",total_count-1)


    # # 去除重复:error/drop_duplicates把nan也计算成重复
    # duplicates = filtered_df[filtered_df['title_pub'].duplicated(keep='first')] 
    # mask = ~duplicates['title_pub'].isna()
    # duplicates = duplicates[mask]  
    # titles_to_drop = duplicates['title_pub']  
    # rows_to_drop = filtered_df[filtered_df['title_pub'].isin(titles_to_drop) &   
    #                            filtered_df['title_pub'].duplicated(keep='first')]  
    # filtered_df = filtered_df[~filtered_df.index.isin(rows_to_drop.index)] 
    # duplicates_count = duplicates['title_pub'].value_counts().sum()
    # print("重复的条目数：",duplicates_count) 
    
    
    # 去掉完全重复
    filtered_df=filtered_df.drop_duplicates()
    

    # 删除title_pub列中以"["开头的行
    title_pub_count=filtered_df['title_pub'].str.startswith('[', na=False).sum()
    print("title_pub中以'['开头的条目数：",title_pub_count)
    filtered_df = filtered_df[~filtered_df['title_pub'].str.startswith('[', na=False)]
    
    
    # 删除Journal/Book列中含有"Rev"的行
    journal_count=filtered_df['Journal/Book'].str.contains('Rev', na=False).sum()
    print("Journal/Book中以'Rev'开头的条目数：",journal_count) 
    filtered_df = filtered_df[~filtered_df['Journal/Book'].str.contains('Rev', na=False)]
    print("过滤完的条目数：",len(filtered_df)-1)
    
    # if total_count==duplicates_count+title_pub_count+journal_count+len(filtered_df):
    #     print("检查通过！！！")

    if total_count==title_pub_count+journal_count+len(filtered_df):
        print("检查通过！！！")
        
        
    # 将筛选后的数据输出到新的CSV文件
    filtered_df.to_csv('./newest/filtered_result.csv', index=False, encoding="utf-8")


def remove_duplicates():
    dir_path = os.path.abspath("../newest")

    if not os.path.exists(dir_path):
        dir_path = os.path.abspath("./newest")

    filename = os.path.join(dir_path, 'join_result(all).csv')
    output_csv = os.path.join(dir_path, 'join_result(all)_output.csv')

    with open(filename, 'r', encoding='utf8', newline='') as infile:
        reader = csv.reader(infile)
        # 使用集合来存储唯一的行，因为集合不允许重复的元素
        seen = set()
        with open(output_csv, 'w', encoding='utf8', newline='') as outfile:
            writer = csv.writer(outfile)
            for row in reader:
                # 将行转换为元组，因为列表是可变的，不能作为集合的元素
                if tuple(row) not in seen:
                    seen.add(tuple(row))
                    writer.writerow(row)

    os.remove(filename)

    # 将去重后的文件重命名为原始文件名
    os.rename(output_csv, filename)


def main():
    # nbci 数据切分 PubmedID 、journal
    print("start split")
    split_data()

    # 1表、3表按照PubmedID合并
    print("start join PubmedID")
    join_data()

    # join表去重
    remove_duplicates()

if __name__ == '__main__':
    main()
    
    
    
    