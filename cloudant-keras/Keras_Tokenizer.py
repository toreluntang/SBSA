import argparse
# from keras.preprocessing.text import Tokenizer, one_hot
from nltk import pos_tag
from nltk import WordNetLemmatizer, sent_tokenize, RegexpTokenizer
from nltk.corpus import stopwords, wordnet
import time


def get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return ''


def nltk_preprocess(text):
    text = text.lower()

    for sent in sent_tokenize(text):
        words_filtered = []

        for word in RegexpTokenizer(r'\w+').tokenize(sent):
            if word not in set(stopwords.words('english')):
                words_filtered.append(word)

        word_lemmatized = []

        for (word, tag) in pos_tag(words_filtered):
            tag = get_wordnet_pos(tag)
            word = WordNetLemmatizer().lemmatize(word, tag)
            word_lemmatized.append(word)

            # print(word_lemmatized)


test_text = "The quick brown fox jumped over the! slow turtle. Mr brown jumps and became the slowest of the turtles."

def measure_time():
    time_taken = []
    for i in range(0, 1000):
        start = time.time()
        nltk_preprocess(test_text)
        end = time.time()
        time_taken.append(end - start)

    return time_taken

l = measure_time()
print(l)

print(float(sum(l)) / max(len(l), 1))




#
# def preprocessing_tokenize(X, num_words=1000):
#     _tokenizer = Tokenizer(num_words=num_words,
#                            filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
#                            lower=True,
#                            split=" ",
#                            char_level=False)
#
#     _tokenizer.fit_on_texts(X)
#     _X = _tokenizer.texts_to_matrix(X)
#     return _X


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--input_string', dest='input_string', help='Input string')
#     args = parser.parse_args()
#
#     print(preprocessing_tokenize([args.input_string]))
