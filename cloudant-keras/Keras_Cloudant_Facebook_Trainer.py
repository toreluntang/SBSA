import argparse
import pprint
from cloudant.client import Cloudant
import numpy as np

#Keras imports
from keras.utils import np_utils
from keras.models import Sequential
from keras.layers import Dense, Activation, Dropout, Convolution2D, Convolution1D, Embedding, GlobalMaxPooling1D, Conv1D, LSTM, Flatten
from keras import losses
from keras import optimizers
from keras.preprocessing.text import Tokenizer


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
    _model = Sequential()
    _model.add(Dense(units=hidden_dims, input_shape=(X.shape[1:]), activation=activation_func))
    _model.add(Dropout(.2))
    _model.add(Dense(units=hidden_dims, activation=activation_func))
    _model.add(Dropout(.2))
    _model.add(Dense(units=hidden_dims, activation=activation_func))
    _model.add(Dropout(.2))
    _model.add(Dense(units=hidden_dims, activation=activation_func))
    # model.add(Dropout(.2))
    # model.add(Dense(units=hidden_dims, activation='relu'))

    # output to one unit
    _model.add(Dense(units=2, activation='softmax'))

    _model.compile(loss=losses.categorical_crossentropy, optimizer=optimizers.adadelta(), metrics=['accuracy'])

    return _model


def fit_eval_keras_model(divider, model, X, Y, batch_size=32, epochs=20):
    train_validate_percent = (int)(len(X) * divider)
    print("Train validate percent = {}".format(train_validate_percent))

    model.fit(X[:train_validate_percent], Y[:train_validate_percent], batch_size=batch_size, epochs=epochs)

    _loss, _score = model.evaluate(X[train_validate_percent:], Y[train_validate_percent:])
    return _loss, _score



def preprocessing_tokenize(X, num_words=1000):
    _tokenizer = Tokenizer(num_words=num_words,
                          filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
                          lower=True,
                          split=" ",
                          char_level=False)

    _tokenizer.fit_on_texts(X)
    _X = _tokenizer.texts_to_matrix(X)
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


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--cloudant_acc', dest='cloudant_account', help='Cloudant account')
    parser.add_argument('--cloudant_dbs',  dest='cloudant_databases', help='Cloudant databases', type=own_list)
    parser.add_argument('--cloudant_pass', dest='cloudant_password', help='Cloudant password')

    args = parser.parse_args()

    dbs = init_cloudant_client(args.cloudant_account, args.cloudant_databases, args.cloudant_password)



    # X and Y is for the NN. X is the data. Y is the target.
    X, Y = preprocessing_transform_to_X_Y(dbs)

    #Filtered X, Y so there is an evenly distribution betweeen negative and positive
    X, Y = filter_to_balance_data(X, Y)

    X = preprocessing_tokenize(X)
    Y = preprocessing_categorize(Y)

    model = gen_keras_model(X, 50)
    print("Model summary:")
    print(model.summary())

    loss, score = fit_eval_keras_model(0.90, model, X, Y, epochs=10)
    print("")
    print("")
    print("Loss[{}], Score[{}]".format(loss, score))

