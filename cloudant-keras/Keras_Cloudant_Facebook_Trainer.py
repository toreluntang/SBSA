import argparse
import pprint
from cloudant.client import Cloudant
import numpy as np
import os

#Keras imports
from keras.utils import np_utils
from keras.models import Sequential
from keras.layers import Dense, Activation, Dropout, Convolution1D, GlobalMaxPooling1D, MaxPooling1D, Conv1D, LSTM, Flatten
from keras import losses
from keras import optimizers
from keras.preprocessing.text import Tokenizer, one_hot
from keras.layers.embeddings import Embedding
from keras.preprocessing import sequence
from keras.preprocessing.sequence import pad_sequences
from keras.datasets import imdb

import ssl

ssl._create_default_https_context = ssl._create_unverified_context

# 71 percent
# OPTIMIZER = optimizers.adam()
# MAX_SEQUENCE_LENGTH = 150
# NUM_WORDS = 5000
# LSTM_SIZE = 50
# EPOCHS = 3


# 72 percent
OPTIMIZER = optimizers.adam()
MAX_SEQUENCE_LENGTH = 150
NUM_WORDS = 10000
LSTM_SIZE = 50
EPOCHS = 2

# 71 percent
# OPTIMIZER = optimizers.adam()
# MAX_SEQUENCE_LENGTH = 300
# NUM_WORDS = 10000
# LSTM_SIZE = 50
# EPOCHS = 2

# 70 percent
# OPTIMIZER = optimizers.adam()
# MAX_SEQUENCE_LENGTH = 150
# NUM_WORDS = 10000
# LSTM_SIZE = 50
# EPOCHS = 4


#Gives around 69
# OPTIMIZER = optimizers.adadelta()
# MAX_SEQUENCE_LENGTH = 150
# NUM_WORDS = 7000
# LSTM_SIZE = 70
# EPOCHS = 2




def init_cloudant_client(account, database_names, password):
    """

    :param account:
    :param database_names:
    :param password:
    :return: Cloudant client and database
    """
    client = Cloudant(account, password, account=account, connect=True)

    try:
        dbs = []
        for db_name in database_names:
            dbs.append(client[db_name])
        return dbs
    except KeyError as ke:
        print("Database not found. Please fix database name [{}]".format(ke))


def reactions(doc):
    # Negative
    angry = doc['ANGRY']['summary']['total_count']
    sad = doc['SAD']['summary']['total_count']

    # Positive
    love = doc['LOVE']['summary']['total_count']
    haha = doc['HAHA']['summary']['total_count']

    # Neutral
    wow = doc['WOW']['summary']['total_count']
    return love, haha, wow, sad, angry


def content(doc):
    # Content
    try:
        msg = doc['message']
    except:
        msg = ""

    try:
        name = doc['name']
    except:
        name = ""

    try:
        desc = doc['description']
    except:
        desc = ""

    return msg, name, desc


def combine_content(msg, name, desc):
    return msg + " " + name + " " + desc


def determine_category(love, haha, wow, sad, angry):
    """
    if love and haha is more than sad and angry then it is positive
    otherwise it is negative. Positive = 1. Negative = 0.
    :param love:
    :param haha:
    :param wow: not used yet
    :param sad:
    :param angry:
    :return: An integer. 0 or 1. Negative or positive.
    """
    _positive = love + haha
    _negative = sad + angry
    if _positive > _negative:
        return 1
    else:
        return 0


def preprocessing_transform_to_X_Y(dbs, verbose=True):

    _X = []
    _Y = []
    for db in dbs:
        for doc in db:

            # extracts the reactions from the document
            love, haha, wow, sad, angry = reactions(doc)

            # extracts the content from the document
            msg, name, desc = content(doc)

            # merges the str
            data = combine_content(msg, name, desc)

            # Gives 0 if negative or 1 if positive.
            category = determine_category(love, haha, wow, sad, angry)

            _X.append(data)
            _Y.append(category)

    if verbose:
        print("Size of X = {}. Size of Y = {}.".format(len(_X), len(_Y)))

    return _X, _Y


def trim(overweight, samples, X, Y):
    _overweight = abs(overweight)
    while _overweight:
        del X[samples[_overweight]]
        del Y[samples[_overweight]]
        _overweight -= 1
    return X, Y


def filter_to_balance_data(X, Y):
    _zeros = ([i for i,x in enumerate(Y) if (x == 0)])
    _ones = ([i for i,x in enumerate(Y) if (x == 1)])

    if len(_zeros) == 0 or len(_ones) == 0:
        return

    #if overweight > 0 then there is more negative. if overweight < 0 then more positive
    overweight = len(_zeros) - len(_ones)

    if overweight > 0:
        return trim(overweight, _zeros, X, Y)
    elif overweight < 0:
        return trim(overweight, _ones, X, Y)
    else:
        return X, Y #Already perfectly balanced.


def gen_keras_model(X, hidden_dims, activation_func="relu"):
    # _model.add(MaxPooling1D(pool_size=5, strides=None, padding='valid'))

    # Convolution
    kernel_size = 5
    filters = 64
    pool_size = 4


    _model = Sequential()
    _model.add(Embedding(NUM_WORDS, 64, input_length=MAX_SEQUENCE_LENGTH))
    _model.add(Conv1D(filters=32, kernel_size=3, padding='same', activation='relu'))
    _model.add(MaxPooling1D(pool_size=3))
    _model.add(LSTM(LSTM_SIZE))
    _model.add(Dropout(0.2))
    _model.add(Dense(32, activation='relu'))
    _model.add(Dense(2, activation='sigmoid'))
    _model.compile(loss='binary_crossentropy', optimizer=OPTIMIZER, metrics=['accuracy'])
    print(_model.summary())


    # _model.add(Conv1D(64, 3, padding='same', activation=activation_func))
    # _model.add(MaxPooling1D(pool_size=4))
    # _model.add(LSTM(70))
    # # _model.add(Conv1D(32, 3, padding='same', activation=activation_func))
    # # _model.add(MaxPooling1D(pool_size=4))
    # # _model.add(LSTM(70))
    # # _model.add(Conv1D(16, 3, padding='same', activation=activation_func))
    # # _model.add(Flatten())
    # _model.add(Dropout(0.2))
    # _model.add(Dense(10,activation=activation_func))
    # _model.add(Dropout(0.2))
    # _model.add(Dense(2,activation='softmax'))
    # _model.compile(loss='binary_crossentropy', optimizer=optimizers.adadelta(), metrics=['accuracy'])

    return _model


def fit_eval_keras_model(divider, model, X, Y, batch_size=64, epochs=2):
    #_X = sequence.pad_sequences(X, maxlen=2000) #fix
    train_validate_percent = (int)(len(X) * divider)
    print("Train validate percent = {}".format(train_validate_percent))

    model.fit(X[:train_validate_percent], Y[:train_validate_percent], batch_size=batch_size, epochs=epochs)

    _loss, _score = model.evaluate(X[train_validate_percent:], Y[train_validate_percent:])
    return _loss, _score


def preprocessing_tokenize(X, num_words=160):
    # _tokenizer = Tokenizer(num_words=NUM_WORDS,
    #                       filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
    #                       lower=True,
    #                       split=" ",
    #                       char_level=False)
    #
    # _tokenizer.fit_on_texts(X)
    # print("There where found {} unique tokens. ".format(len(_tokenizer.word_index)))
    # _X = _tokenizer.texts_to_matrix(X)


    #Think this is num_words
    _X = []
    for x in X:
        _X.append(one_hot(x,NUM_WORDS,split=' '))

    _X = pad_sequences(_X, maxlen=MAX_SEQUENCE_LENGTH)

    # embeddings_index = {}
    # _dir = "/Users/alexanderengelhardt/Downloads/glove.6B/"
    # f = open(os.path.join(_dir, 'glove.6B.50d.txt'), 'rb')
    # for line in f:
    #     values = line.split()
    #     word = values[0]
    #     coefs = np.asarray(values[1:], dtype='float32')
    #     embeddings_index[word] = coefs
    # f.close()

    # dunno what this is
    # EMBEDDING_DIM = 50
    #
    # embedding_matrix = np.zeros((len(_tokenizer.word_index) + 1, EMBEDDING_DIM))
    # for word, i in _tokenizer.word_index.items():
    #     embedding_vector = embeddings_index.get(word)
    #     if embedding_vector is not None:
    #         # words not found in embedding index will be all-zeros.
    #         embedding_matrix[i] = embedding_vector
    #
    # embedding_layer = Embedding(len(_tokenizer.word_index) + 1,
    #                             EMBEDDING_DIM,
    #                             weights=[embedding_matrix],
    #                             input_length=MAX_SEQUENCE_LENGTH,
    #                             trainable=False)
    
    return _X


def preprocessing_categorize(Y):
    _Y = np.array(Y)
    _Y = np_utils.to_categorical(_Y)
    return _Y



def own_list(val):
    try:
        return str(val).replace(" ", "").split(",")
    except ValueError:
        raise argparse.ArgumentParser("Value {} has to be in for \"cnn,msnbc,breitbart, foxnews\"".format(val))


def shuffle_data(X, Y, seed = 10):
    np.random.seed(seed)
    np.random.shuffle(X)
    np.random.seed(seed)
    np.random.shuffle(Y)
    return X,Y


def train_and_save(cloudant_acc, cloudant_dbs, cloudant_password, filename, hidden_dims=50, epochs=10):
    _dbs = init_cloudant_client(cloudant_acc, cloudant_dbs, cloudant_password)
    
    # X and Y is for the NN. X is the data. Y is the target.
    _X, _Y = preprocessing_transform_to_X_Y(_dbs)
    
    #Filtered X, Y so there is an evenly distribution betweeen negative and positive
    _X, _Y = filter_to_balance_data(_X, _Y)

    # Shuffles data points
    _X, _Y = shuffle_data(_X, _Y)
    
    _X = preprocessing_tokenize(_X)
    _Y = preprocessing_categorize(_Y)

    
    _model = gen_keras_model(_X, hidden_dims)
    _loss, _score = fit_eval_keras_model(0.95, _model, _X, _Y, epochs=EPOCHS)
    
    _fname = filename+str(_score)+".h5"
    _model.save(_fname)
    return _fname, _loss, _score, _model

if __name__ == '__main__':

    # parser = argparse.ArgumentParser()
    # parser.add_argument('--cloudant_acc', dest='cloudant_account', help='Cloudant account')
    # parser.add_argument('--cloudant_dbs',  dest='cloudant_databases', help='Cloudant databases', type=own_list)
    # parser.add_argument('--cloudant_pass', dest='cloudant_password', help='Cloudant password')
    #
    # args = parser.parse_args()
    #
    # _fname, _loss, _score, _model = train_and_save(args.cloudant_account, args.cloudant_databases, args.cloudant_password, 'model_')
    # print("Loss: {}, Score: {}".format(_loss, _score))

    test_text = ["The quick brown fox jumped over the! slow turtle. Mr brown jumps and became the slowest of the turtles."]

    _tok = Tokenizer(num_words=30,
                          filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
                          lower=True,
                          split=" ",
                          char_level=False)
    _tok.fit_on_texts(test_text)

    print(_tok.word_index)
    print(len(_tok.word_index))

    mans = one_hot(test_text[0], 30, split=' ')


    map = {}

    for i, word in enumerate(test_text[0].split()):
        val = map.get(mans[i])
        if val is None:
            map[mans[i]] = [word]
        else:
            val.append(word)
            map[mans[i]] = val

    print(map)
    print(mans)

    embeddings_index = {}
    glove_data = '/Users/alexanderengelhardt/Downloads/glove.6B/glove.6B.50d.txt'
    f = open(glove_data)
    for line in f:
        values = line.split()
        word = values[0]
        value = np.asarray(values[1:], dtype='float32')
        embeddings_index[word] = value
    f.close()

    print('Loaded %s word vectors.' % len(embeddings_index))

    # embedding_dimension = 10
    # word_index = tokenizer.word_index

