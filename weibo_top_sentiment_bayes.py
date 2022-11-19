
import jieba
import re
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn import metrics
import pickle




def load_corpus(path):
    """
    加载语料库
    """
    data = []
    with open(path, "r", encoding="utf8") as f:
        for line in f:
            [_, seniment, content] = line.split(",", 2)
            content = processing(content)
            data.append((content, int(seniment)))
    return data

def processing(text):
    """
    数据预处理, 可以根据自己的需求进行重载
    """
    # 数据清洗部分
    text = re.sub("\{%.+?%\}", " ", text)           # 去除 {%xxx%} (地理定位, 微博话题等)
    text = re.sub("@.+?( |$)", " ", text)           # 去除 @xxx (用户名)
    text = re.sub("【.+?】", " ", text)              # 去除 【xx】 (里面的内容通常都不是用户自己写的)
    text = re.sub("\u200b", " ", text)              # '\u200b'是这个数据集中的一个bad case, 不用特别在意
    # 分词
    words = [w for w in jieba.lcut(text) if w.isalpha()]
    # 对否定词`不`做特殊处理: 与其后面的词进行拼接
    while "不" in words:
        index = words.index("不")
        if index == len(words) - 1:
            break
        words[index: index+2] = ["".join(words[index: index+2])]  # 列表切片赋值的酷炫写法
    # 用空格拼接成字符串
    result = " ".join(words)
    return result


TRAIN_PATH = "./weibodata/train.txt"
TEST_PATH = "./weibodata/test.txt"

def train():
    # 停用词
    stopwords = []
    with open("weibodata/stopwords.txt", "r", encoding="utf8") as f:
        for w in f:
            stopwords.append(w.strip())

    # 分别加载训练集和测试集
    train_data = load_corpus(TRAIN_PATH)
    test_data = load_corpus(TEST_PATH)
    df_train = pd.DataFrame(train_data, columns=["words", "label"])
    df_test = pd.DataFrame(test_data, columns=["words", "label"])

    vectorizer = CountVectorizer(token_pattern='\[?\w+\]?', 
                             stop_words=stopwords)
    X_train = vectorizer.fit_transform(df_train["words"])
    y_train = df_train["label"]

    X_test = vectorizer.transform(df_test["words"])
    y_test = df_test["label"]

    clf = MultinomialNB()
    clf.fit(X_train, y_train)

    # 在测试集上用模型预测结果
    y_pred = clf.predict(X_test)

    print(metrics.classification_report(y_test, y_pred))
    print("准确率:", metrics.accuracy_score(y_test, y_pred))

    strs = ['#奥运会##艺术体操团体全能决赛##中国队艺术体操团体全能第四#探班艺体小姐姐们训练！小姐姐们真的好美呢！一起为她们加油',
             '艺术体操团体全能决赛#妈耶！仙女们都太厉害了吧！手里的球也太听话了',
             '在今天的#艺术体操团体全能决赛#中，#中国队获艺术体操团体全能第四名# ，五位平均年龄22岁的中国姑娘完美演绎了“中国风”。网友盛赞，“敦煌飞天好美！”  不少网友回忆起2008北京奥运会上中国艺术体操队的“中国风”，太经典了，一起来回顾下',
             '#货拉拉跳车事件司机妻子发声#舆论不能干涉司法，司法也不能受舆论干涉，该怎么判就怎么判，一直把人关着算怎么回事',
             '阿里 破冰文化#看到阿里被爆出来的“破冰文化”，觉得又震惊，又在情理之中。...反之，过度纵容、默认，让这种价值观成为一种常态，成为“正常”，这世界就太让人失望了。']
    words = [processing(s) for s in strs]
    vec = vectorizer.transform(words)
    output = clf.predict_proba(vec)
    print(strs)
    print(output)
    print(clf.predict(vec))

    with open('bayes_model.pkl', 'wb') as f:
        pickle.dump([clf, vectorizer], f)

class BayesSentiment(object):
    def __init__(self):
        with open('bayes_model.pkl', 'rb') as f:
            self.clf, self.vectorizer = pickle.load(f)

    def predict(self, sentence):
        sentenceprocessed = [processing(sentence)]
        #print(type(sentenceprocessed), sentenceprocessed)
        vec = self.vectorizer.transform(sentenceprocessed)
        return self.clf.predict_proba(vec)[0][1]


if __name__ == '__main__':

    train()    
