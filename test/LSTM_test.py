from keras.models import Sequential
from keras import losses
from keras import optimizers
from keras import initializers
from keras.layers import Dense, Activation, Dropout, Convolution2D, Convolution1D, Embedding, GlobalMaxPooling1D, \
    Conv1D, LSTM, Flatten
from keras.preprocessing import sequence
from keras.utils import np_utils
import numpy as np
from cloudant.client import Cloudant
from keras.preprocessing.text import Tokenizer
from random import shuffle
import config

client = Cloudant(config.dbacc, config.dbpass, account=config.dbacc, connect=True)
db = client[config.dbname]

ddoc = db.get_view_result(ddoc_id='view', view_name='reaction_aggregated', reduce=True, group=True, raw_result=True)

# Shuffle input
shuffle(ddoc['rows'])


message_reactions = {}

for d in ddoc['rows']:
    # If message doesnt exist as key in message_reactions, create it with empty list
    # else append the new reaction to it
    msg = d['key'][0]
    emoji = d['key'][1]
    count = d['value']

    emoji_count = {emoji:count}


    if msg is not None:
        if msg in message_reactions:
            message_reactions[msg].update(emoji_count)
        else:
            message_reactions[msg] = emoji_count


to_be_del = []
num_of_angries = 0
for msg in message_reactions:
    if msg is not None:

        try:
            e = message_reactions[msg]
            like = e['LIKE']

            try:
                angry = e['ANGRY']
            except KeyError:
                angry = 0

            try:
                sad = e['SAD']
            except KeyError:
                sad = 0

            try:
                wow = e['WOW']
            except KeyError:
                wow = 0

            try:
                love = e['LOVE']
            except KeyError:
                love = 0

            try:
                haha = e['HAHA']
            except KeyError:
                haha = 0

            if like < 100 and (angry+sad > love+haha+wow) and angry+sad >= 5:
                message_reactions[msg].update({'is_angry' : 1})
                num_of_angries += 1
            elif like > 85:
                to_be_del.append(msg)
            else:
                message_reactions[msg].update({'is_angry': 0})

        except KeyError:
            if like > 85:
                to_be_del.append(msg)
            else:
                message_reactions[msg].update({'is_angry': 0})

        # print("Msg: {} - {}".format(msg, message_reactions[msg]))
        # print("")

for d in to_be_del:
    message_reactions.pop(d)

print(len(message_reactions))
print(num_of_angries)


#Try shuffling the messages


#Starting ML

# set parameters:
# max_features = 5000
# maxlen = 400
# batch_size = 32
# embedding_dims = 50
# filters = 250
# kernel_size = 3
# hidden_dims = 250
# epochs = 2

# set parameters:
max_features = 5000
maxlen = 400
batch_size = 32
embedding_dims = 50
filters = 256
kernel_size = 3
hidden_dims = 16
epochs = 6

X = []
Y = []

for msg in message_reactions:
    if msg is not None:
        X.append(msg)
        Y.append([message_reactions[msg]['is_angry']])

Y = np.array(Y)
Y = np_utils.to_categorical(Y)

tokenizer = Tokenizer(num_words=None,
                                   filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
                                   lower=True,
                                   split=" ",
                                   char_level=False)

tokenizer.fit_on_texts(X)
X = tokenizer.texts_to_matrix(X)

# X = sequence.pad_sequences(X, maxlen=maxlen)

model = Sequential()
# model.add(Embedding(max_features,
#                     embedding_dims,
#                     input_length=maxlen))
# model.add(Dense(units=hidden_dims, input_shape=(X.shape[1:]), activation='relu'))
# model.add(LSTM(embedding_dims, return_sequences=True))
# model.add(LSTM(25, return_sequences=True))
# model.add(LSTM(10))
# model.add(LSTM(32, return_sequences=True, stateful=True))
# model.add(LSTM(32, stateful=True))

# model.add(Dense(units=hidden_dims, activation='relu'))
model.add(Dense(units=hidden_dims, input_shape=(X.shape[1:]), activation='relu'))
model.add(Dropout(.2))
model.add(Dense(units=hidden_dims, activation='relu'))
model.add(Dropout(.2))
model.add(Dense(units=hidden_dims, activation='relu'))
model.add(Dropout(.2))
# model.add(Dense(units=hidden_dims, activation='relu'))

#output to one unit
model.add(Dense(units=2, activation='softmax'))


model.compile(loss=losses.categorical_crossentropy, optimizer=optimizers.adam(), metrics=['accuracy'])

train_divider = (int) (len(message_reactions) * 0.9)

model.fit(X[:train_divider],Y[:train_divider], batch_size=batch_size, epochs=epochs)

print(model.summary())

score = model.evaluate(X[train_divider:], Y[train_divider:])

# predictions = model.predict(X[train_divider:])


# cur = 0
# test_data = []
# for msg in message_reactions:
#     # if msg is not None:
#     #     print("#{}: {} - is_angry={}".format(cur, msg[:40], message_reactions[msg]))
#     if cur >= train_divider:
#         test_data.append({msg : message_reactions[msg] })
#     cur += 1
#
# print("")
# print("")
# print(predictions)

print("")
print("")
print("Score: {}".format(score))


# print(len(test_data))
# print(len(score))
#
# curr = 0
# for p,t in zip(predictions, test_data):
#     # print(t)
#     print("#{}: data: {}, Score: {}".format(curr, t, p))
#     curr += 1
