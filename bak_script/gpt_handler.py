# -*- encoding:utf-8 -*-
# @time: 2023/5/21 15:12
# @author: ifeng
import time
import os

from openai import OpenAI

api_key = os.environ.get("OPENAI_API_KEY")
base_url = "https://api.fast-tunnel.one/v1"
model = "gpt-4o-mini"

client = OpenAI(
    api_key=api_key,
    base_url=base_url,
)


def chat(query, history, client=client, model=model):
    history.append({
        "role": "user",
        "content": query
    })
    completion = client.chat.completions.create(
        model=model,
        messages=history,
        stream=False,
        temperature=0.3,
    )
    result = completion.choices[0].message.content
    history.append({
        "role": "assistant",
        "content": result
    })
    return result


def get_search_question():
#    file_name = './newest/join_result(all).csv'
    file_name = './newest/filtered_result.csv'
    with open(file_name, 'r', encoding='utf8', errors='ignore') as f1, \
            open("./newest/PubtoGEO_data_all.csv", mode="a", encoding="utf8", errors='ignore') as f2:
        line_num = 0
        for line in f1:
            line_num += 1
            line = line.strip('\n').strip(',')
            if line != "title_pub,journal_pub,PubDate,PubmedID,Url,Abstract,title_geo,Accession,Organism,Summary,Overall_design,journal_geo":
                # over_design = line.split(',')[-3].strip()
                # summary = line.split(',')[-4].strip()
                question = """
你是一个很擅长读文献的生物学的博士毕业生，你能从文字中快速的找出我需要的关键信息，请先阅读我的文字提取实例：
Title_pub	Journal	PubDate	PubmedID	Url	Abstract	Title_GEO	Accession	Organism	Summary	Overall_design
Dissecting the multicellular ecosystem of metastatic melanoma by single-cell RNA-seq.	Science	2016 Apr 8	27124452	https://pubmed.ncbi.nlm.nih.gov/27124452/	To explore the distinct genotypic and phenotypic states of melanoma tumors， we applied single-cell RNA sequencing (RNA-seq) to 4645 single cells isolated from 19 patients， profiling malignant， immune， stromal， and endothelial cells. Malignant cells within the same tumor displayed transcriptional heterogeneity associated with the cell cycle， spatial context， and a drug-resistance program. In particular， all tumors harbored malignant cells from two distinct transcriptional cell states， such that tumors characterized by high levels of the MITF transcription factor also contained cells with low MITF and elevated levels of the AXL kinase. Single-cell analyses suggested distinct tumor microenvironmental patterns， including cell-to-cell interactions. Analysis of tumor-infiltrating T cells revealed exhaustion programs， their connection to T cell activation and clonal expansion， and their variability across patients. Overall， we begin to unravel the cellular ecosystem of tumors and how single-cell genomics offers insights with implications for both targeted and immune therapies.	Single cell RNA-seq analysis of melanoma	GSE72056	Homo sapiens	To understand the diversity of expression states within melanoma tumors， we obtained freshly resected samples， dissagregated the samples， sorted into single cells and profiled them by single-cell RNA-seq.	Tumors were disaggregated， sorted into single cells， and profiled by Smart-seq2.*Raw data files absent for samples GSM1851356 and GSM1851494.***Submitter declares reads will be made available through dbGaP.**Please note that the processed data file (melanoma_single_cell_revised_v2.txt) contains two additional sample characteristis ("classification (based on inferred cnvs)" and "cell types for non-malignant cells").

基于以上文字提取信息表格，以下面的形式展示： 
pubmedID	title	accessionNumber	journal	numberOfCells	taxonomyID	tissue	cancer	disease	methodology	neuroscience	developmentalBiology	immunology	cellAtlas	libraryPreparationMethod
27124452	Dissecting the multicellular ecosystem of metastatic melanoma by single-cell RNA-seq	GSE72056	Science	4645	9606	['skin', 'spleen', 'lymph node', 'intestine']	True	True	False	False	False	False	True	Smart-seq2

然后帮我读下面一段文字，提取同样的表格：
Title_pub	Journal	PubDate	PubmedID	Url	Abstract	Title_GEO	Accession	Organism	Summary	Overall_design
%s

提取的表格一共15个字段(pubmedID, title, accessionNumber, journal, numberOfCells, taxonomyID, tissue, cancer, disease, methodology, neuroscience, developmentalBiology, immunology, cellAtlas, libraryPreparationMethod),
提取字段要求如下：
    1.pubmedID
	2.title
	3.accessionNumber：打开pubmedID_pdf文件，从pdf文件的Data Availability部分找到
	4.journal
	5.numberOfCells：可以从Abstract中找，如果Abstract中没有，可以打开pubmedID_pdf文件，从pdf文件中找一下，一般会出现在Results的前半部分
	6.taxonomyID
	7.tissue：可以从Title或Abstract中找，如果Title或Abstract中没有，可以打开pubmedID_pdf文件，从pdf文件中找一下，一般会出现在Methods里面，填写内容请从这里选择：
                HSC/progenitor cells
                Erythroid cells-IPSC-derived 
                Mixture-fetal
                Bone marrow,Liver-fetal
                Lymph node-cervical 
                Mesenchymal cells
                Bone marrow-fetal
                Blood,Lung
                Trachea
                Embryos
                Tongue
                Bladder
                Decidua basalis
                Vasculature
                Liver,Thymus
                Blood,Bone marrow,Liver
                Muscle
                Yolk sac
                pancreas
                Nasopharynx
                Trophoblast
                Stromal-colon
                Blood,Spleen
                T cells
                Ovary
                Yolk sac,liver,HESC
                Cultured cell-Calu-3
                Lymph node-inguinal
                Cultured cell-HUVECs
                Kidney, Skin-fetal
                Immune cells
                Epithelial cells-Mix
                Placenta
                Epithelial cell
                Blood,Bone marrow
                Cultured cell-H1299
                Esophagus
                Islet of Langerhans
                Oral
                Tonsil
                Skin
                Spleen
                Epithelial cells-colon
                Cultured cell-A549
                Adipose
                Cultured cell-K562
                Immune cells-colon
                lymph node
                Liver-fetal
                NK/T cells
                Endometrium
                Uterine tube
                Prostate
                Airway
                Cultured cell-MCF7
                Blood,Placenta
                Bone marrow
                Liver
                Thymus
                Gland
                Myeloid cells
                Gastrointestinal
                Kidney
                Breast
                Heart
                Lung
                Mixture-embryo
                Eye
                Mixture
                Blood
                Brain
    8.Cancer：可以从Title或Abstract中判断
    9.Disease：可以从Title或Abstract中判断
    10.Methodology：可以从Title或Abstract中判断，如果是算法预测，这里给True，否则给False
    11.Neuroscience：可以从Title或Abstract中判断
    12.Developmental biology：可以从Title或Abstract中判断
    13.Immunology：可以从Title或Abstract中判断，如果Title或Abstract中没有，可以打开pubmedID_pdf文件，从pdf文件中找一下，如果文章中分析了很多免疫细胞，这里给True
    14.Cell atlas：可以从Title或Abstract中判断，如果Title或Abstract中没有出现“Cell atlas”，这里给False，反之给True。同时考虑5.numberOfCells中的数值大于10000才给True，反之给False
    15.Library preparation method：可以从Title或Abstract中找，如果Title或Abstract中没有，可以打开pubmedID_pdf文件，从pdf文件中找一下，一般会出现在Methods里面，填写内容请从这里选择：
            BD Rhapsody Whole Transcriptome Analysis
            mCT-seq
            microwell-seq
            QUARTZ-seq
            Seq-Well S3
            SPLiT-seq
            BD Rhapsody Targeted mRNA
            CEL-seq2
            MARS-seq
            sci-RNA-seq
            Seq-Well
            Smart-seq
            STRT-seq
            Abseq
            Fluidigm C1
            sci-Plex
            TruDrop
            10x (CITE-seq)
            10x multiome
            Drop-seq
            Smart-seq2
            10x，Smart-seq2
            Slide-seqV2
            10x，10x multiome
            Visium Spatial Gene Expression
            10x
要求：
    1、请严格按照我给你的提示词顺序进行整理输出, 以上字段一个不要多, 一个不要少,一定要按顺序输出!!!
	2、tissue和Library preparation method字段填写的时候，不要填写规定内容以外的内容，比如下面的内容，就不要输出：
        - "根据您提供的文本，以下是提取的信息表格："
        - "以下是提取的信息表格："
        - "请注意，某些字段（如 accessionNumber 和 numberOfCells）是根据假设值填写的，因为没有提供相应的 PDF 文件内容。请根据实际情况进行调整。"
    3、输出的内容要转成我可以保存到csv格式
""" % (line)

                print(f"page::: {line_num}")

                role_def = "生物学博士毕业生"

                history = [
                   {
                        "role": "system",
                        "content": role_def
                    }
                ]
                # 调用gpt接口
                result = chat(question, history, client=client, model=model)
                if line_num == 2:
                    print("res::::", result)
                #elif line_num == 80:
                #    break
                elif line_num % 3 == 0:
                    result = result.replace(
                        "pubmedID,title,accessionNumber,journal,numberOfCells,taxonomyID,tissue,cancer,disease,methodology,neuroscience,developmentalBiology,immunology,cellAtlas,libraryPreparationMethod",
                        "").strip()
                    result = result.replace('```', '')
                    result = result.replace('```csv', '')
                    result = result.replace('csv', '')

                    print("res::::", result)

                    f2.flush()
                    time.sleep(1)
                else:
                    result = result.replace(
                        "pubmedID,title,accessionNumber,journal,numberOfCells,taxonomyID,tissue,cancer,disease,methodology,neuroscience,developmentalBiology,immunology,cellAtlas,libraryPreparationMethod",
                        "").strip()
                    result = result.replace('```', '')
                    result = result.replace('```csv', '')

                    print("res::::", result)

                result = result.replace('csv', '')
                if result == "  ":
                    print("null")
                    continue
                elif result == "":
                    print("null")
                    continue
                elif result == "\n":
                    print("null")
                    continue
                result = result.replace("待查找", "null")
                result = result.replace("未提供", "null")
                result = result.replace("未知", "null")
                result = result.replace("不详", "null")
                f2.write(result)
                f2.write('\n')


def main():
    get_search_question()

# if __name__ == '__main__':
#     main()

