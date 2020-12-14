# -*- coding: utf-8 -*-

"""

 @Time    : 2019/1/29 14:34
 @Author  : MaCan (ma_cancan@163.com)
 @File    : client.py
 some code come from <https://hanxiao.github.io>
"""

import sys
import threading
import time
import uuid
import warnings
import pickle
from collections import namedtuple
import re
import numpy as np
import zmq
from zmq.utils import jsonapi
# import fsp_extract_helper

def is_Chinese(word):
    '''
    检查是否为中文
    '''
    for ch in word:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False

__all__ = ['__version__', 'BertClient', 'ConcurrentBertClient']

# in the future client version must match with server version
__version__ = '1.7.8'

if sys.version_info >= (3, 0):
    _py2 = False
    _str = str
    _buffer = memoryview
    _unicode = lambda x: x

Response = namedtuple('Response', ['id', 'content'])


class BertClient(object):
    def __init__(self, ip='localhost', port=5555, port_out=5556,
                 output_fmt='ndarray', show_server_config=False,
                 identity=None, check_version=True, check_length=True,
                 timeout=-1, mode='NER',max_seq_length=110, ner_model_dir=None):
        """ A client object connected to a BertServer

        Create a BertClient that connects to a BertServer.
        Note, server must be ready at the moment you are calling this function.
        If you are not sure whether the server is ready, then please set `check_version=False` and `check_length=False`

        :type timeout: int
        :type check_version: bool
        :type check_length: bool
        :type identity: str
        :type show_server_config: bool
        :type output_fmt: str
        :type port_out: int
        :type port: int
        :type ip: str
        :param ip: the ip address of the server
        :param port: port for pushing data from client to server, must be consistent with the server side config
        :param port_out: port for publishing results from server to client, must be consistent with the server side config
        :param output_fmt: the output format of the sentence encodes, either in numpy array or python List[List[float]] (ndarray/list)
        :param show_server_config: whether to show server configs when first connected
        :param identity: the UUID of this client
        :param check_version: check if server has the same version as client, raise AttributeError if not the same
        :param check_length: check if server `max_seq_len` is less than the sentence length before sent
        :param timeout: set the timeout (milliseconds) for receive operation on the client, -1 means no timeout and wait until result returns
        :param mode: mode for bert or ner or etc...
        :param ner_model_dir dir of ner mdoel, should contains label2id.pkl
        """

        self.context = zmq.Context()
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.setsockopt(zmq.LINGER, 0)
        self.identity = identity or str(uuid.uuid4()).encode('ascii')
        self.sender.connect('tcp://%s:%d' % (ip, port))

        self.receiver = self.context.socket(zmq.SUB)
        self.receiver.setsockopt(zmq.LINGER, 0)
        self.receiver.setsockopt(zmq.SUBSCRIBE, self.identity)
        self.receiver.connect('tcp://%s:%d' % (ip, port_out))
        self.max_seq_length=max_seq_length
        self.request_id = 0
        self.timeout = timeout
        self.pending_request = set()
        if type(mode) != str:
            raise ArithmeticError('mode params should be str type, but input mode type is :%s' %type(mode))
        else:
            self.mode = mode

        if output_fmt == 'ndarray':
            self.formatter = lambda x: x
        elif output_fmt == 'list':
            self.formatter = lambda x: x.tolist()
        else:
            raise AttributeError('"output_fmt" must be "ndarray" or "list"')

        self.output_fmt = output_fmt
        self.port = port
        self.port_out = port_out
        self.ip = ip
        self.length_limit = 0

        if check_version or show_server_config or check_length:
            s_status = self.server_status

            if check_version and s_status['server_version'] != self.status['client_version']:
                raise AttributeError('version mismatch! server version is %s but client version is %s!\n'
                                     'consider "pip install -U bert-serving-server bert-serving-client"\n'
                                     'or disable version-check by "BertClient(check_version=False)"' % (
                                         s_status['server_version'], self.status['client_version']))
            # check mode
            if self.mode != s_status['mode']:
                raise ArithmeticError('server mode is:{}, but client mode is :{}'.format(s_status['mode'], self.mode))

            if show_server_config:
                self._print_dict(s_status, 'server config:')

            if check_length:
                self.length_limit = int(s_status['max_seq_len'])

    def close(self):
        """
            Gently close all connections of the client. If you are using BertClient as context manager,
            then this is not necessary.

        """
        self.sender.close()
        self.receiver.close()
        self.context.term()

    def _send(self, msg, msg_len=0):
        self.sender.send_multipart([self.identity, msg, b'%d' % self.request_id, b'%d' % msg_len])
        self.pending_request.add(self.request_id)
        self.request_id += 1

    def _recv(self):
        response = self.receiver.recv_multipart()
        request_id = int(response[-1])
        self.pending_request.remove(request_id)
        return Response(request_id, response)

    def _recv_ndarray(self):
        request_id, response = self._recv()
        if self.mode == 'NER':
            print('NER')
            arr_info, arr_val = jsonapi.loads(response[1]), pickle.loads(response[2])
            # print('arr_info',arr_info)
            # print('arr_val',arr_val)
            assert arr_info['dtype'] == 'str'
            return Response(request_id, arr_val)
        elif self.mode == 'BERT':
            print('BERT')
            arr_info, arr_val = jsonapi.loads(response[1]), response[2]
            X = np.frombuffer(_buffer(arr_val), dtype=str(arr_info['dtype']))
            return Response(request_id, self.formatter(X.reshape(arr_info['shape'])))
        elif self.mode == 'CLASS':
            print('CLASS')
            arr_info, arr_val = jsonapi.loads(response[1]), pickle.loads(response[2])
            # print('*'*20,arr_info['dtype'])
            assert arr_info['dtype'] == 'json'
            return Response(request_id, arr_val)

    @property
    def status(self):
        """
            Get the status of this BertClient instance

        :rtype: dict[str, str]
        :return: a dictionary contains the status of this BertClient instance

        """
        return {
            'identity': self.identity,
            'num_request': self.request_id,
            'num_pending_request': len(self.pending_request),
            'pending_request': self.pending_request,
            'output_fmt': self.output_fmt,
            'port': self.port,
            'port_out': self.port_out,
            'server_ip': self.ip,
            'client_version': __version__,
            'timeout': self.timeout
        }

    def _timeout(func):
        def arg_wrapper(self, *args, **kwargs):
            if 'blocking' in kwargs and not kwargs['blocking']:
                # override client timeout setting if `func` is called in non-blocking way
                self.receiver.setsockopt(zmq.RCVTIMEO, -1)
            else:
                self.receiver.setsockopt(zmq.RCVTIMEO, self.timeout)
            try:
                return func(self, *args, **kwargs)
            except zmq.error.Again as _e:
                t_e = TimeoutError(
                    'no response from the server (with "timeout"=%d ms), please check the following:'
                    'is the server still online? is the network broken? are "port" and "port_out" correct? '
                    'are you encoding a huge amount of data whereas the timeout is too small for that?' % self.timeout)
                if _py2:
                    raise t_e
                else:
                    raise t_e from _e
            finally:
                self.receiver.setsockopt(zmq.RCVTIMEO, -1)

        return arg_wrapper


    @property
    @_timeout
    def server_status(self):
        """
            Get the current status of the server connected to this client

        :return: a dictionary contains the current status of the server connected to this client
        :rtype: dict[str, str]

        """
        self.receiver.setsockopt(zmq.RCVTIMEO, self.timeout)
        self._send(b'SHOW_CONFIG')
        return jsonapi.loads(self._recv().content[1])
    def DBC2SBC(self,ustring):
        """把字符串全角转半角"""
        rstring = ""
        for uchar in ustring:
            inside_code=ord(uchar)
            if inside_code==0x3000:
                inside_code=0x0020
            else:
                inside_code-=0xfee0
            if inside_code<0x0020 or inside_code>0x7e:   #转完之后不是半角字符返回原来的字符
                rstring += uchar
            else:
                rstring += chr(inside_code)
        return rstring
    def add_comma(self,context):
        if '\t' in context:
            while('\t\t' in context):
                context=context.replace('\t\t','\t')
            context=context.replace('\t',',')
        while(',,' in context):
            context=context.replace(',,',',')
        #if context[-1]!='。':
        #    context=context+'。'
        return context
    @_timeout
    def process_seq11(self, texts):
        #seqs = re.split(r"([。!？；])", texts)
        # texts = re.sub('[a-zA-Z0-9\%]','',texts)
        texts = re.sub('[0-9\%]','',texts)

        texts=texts.replace('\n','').replace('\r','').replace('\t','').replace(' ','').replace(' ','').replace('/','').replace(u'\u3000','').replace('\xa0','')
        d = "。"
        seqs =  [e+d for e in texts.split(d) if e]
        tem_all=[]
        #print(seqs)
        seqs_new=[]
        for i in seqs:
            seqs_new.append(i)
            if len(i)>150 and ',' in i:
                seqs_new=seqs_new+[e+d for e in i.split(',') if e]
        for seq_i in seqs_new:
            if '公司' not in seq_i:continue
            rst = self.encode([list(seq_i)], is_tokenized=True)
            print(rst)
            tem=''
            for i,k in enumerate(seq_i):
                if i< len(rst[0])-1:
                    if rst[0][i]=='B-ORG' and tem!='':
                        if tem not in tem_all:
                            #print(111111111,tem)
                            tem_all.append(tem)
                        tem=''
                    if rst[0][i]=='I-ORG' and tem=='':continue
                    if rst[0][i]=='E-ORG' and tem=='':continue
                    if rst[0][i]!='O':
                        #print(k)
                        tem=tem+k
                    if rst[0][i]=='E-ORG' and tem!='':
                        #if tem not in tem_all:
                        #print(222222222222,tem)
                        tem_all.append(tem)
                        tem=''
        return tem_all
    @_timeout
    def process_seq_old(self, texts):
        texts = self.DBC2SBC(texts)
        #seqs = re.split(r"([。!？；])", texts)
        #texts = texts+texts+texts+texts+texts+texts+texts+texts
        # texts = re.sub('[a-zA-Z0-9\%]','',texts)
        texts = re.sub('[0-9\%]','',texts)

        texts=texts.replace('\n','').replace('\r','').replace('\t','').replace(' ','').replace(' ','').replace('/','').replace(u'\u3000','').replace('\xa0','')
        d = "。"
        seqs =  [e+d for e in texts.split(d) if e]
        tem_all=[]
        #print(seqs)
        seqs_new=[]
        seqs_list_new=[]
        for i in seqs:
            seqs_new.append(i)
            if len(i)>300 and ',' in i:
                seqs_new=seqs_new+[e+d for e in i.split(',') if e]
        for seq_i in seqs_new:
            #if '公司' not in seq_i:continue
            #print(seq_i)
            seqs_list_new.append(list(seq_i))
        if seqs_list_new==[]:return []
        print(seqs_list_new)
        rsts = self.encode(seqs_list_new, is_tokenized=True)
        for m,rst in enumerate(rsts):
            #print(rst,seqs_list_new[m])
            tem=''

            for i,k in enumerate(seqs_list_new[m]):
                if i< len(rst)-1:
                    if rst[i]=='B-ORG' and tem!='':
                        if tem not in tem_all:
                            #print(111111111,tem)
                            tem_all.append(tem)
                        tem=''
                    if rst[i]=='I-ORG' and tem=='':continue
                    if rst[i]=='E-ORG' and tem=='':continue
                    if rst[i]!='O':
                        #print(k)
                        tem=tem+k
                    if rst[i]=='E-ORG' and tem!='':
                        #if tem not in tem_all:
                        #print(222222222222,tem)
                        tem_all.append(tem)
                        tem=''
        tem_all=list(set(tem_all))
        return tem_all

    def process_seq(self, texts):
        texts = self.DBC2SBC(texts)

        texts =texts.strip()
        texts=texts.replace('\n','').replace(' ',',').replace('。',',').replace('】','').replace('】','').replace('/','').replace('\u3000','').replace('\xa0','').replace('\r','').replace('.','').replace('\u2002','')#.replace('*','')
        texts = texts.replace(':,',':').replace('。,','。').replace('!,','!').replace('!,','!')

        # texts = re.sub('[a-zA-Z0-9\%]','',texts)
        texts = re.sub('[0-9\%]','',texts)
        texts = self.add_comma(texts)

        d = ","
        seqs =  [e+d for e in texts.split(d) if e]
        #seqs2=[]
        #seq_tem=''
        # for k,i  in enumerate(seqs):
        #     #print(i)
        #     if k<len(seqs)-1 and len(seq_tem)<300 and len(seq_tem+i)<300:
        #         seq_tem=seq_tem+i
        #         #print(1111,seq_tem)
        #     else:
        #         seq_tem=seq_tem.replace('。',',')+'。'
        #         #if seq_tem[0]==',':seq_tem=seq_tem[1:]
        #         if seq_tem[-2:]==',。':seq_tem=seq_tem[:-2]+'。'
        #         #print(22222222,seq_tem)
        #         seqs2.append(seq_tem)
        #         seq_tem=''

        seqs2 = []
        tem=''
        for j,item in enumerate(seqs):
            '''将不满足最大长度的句子拼接起来'''
            if j==0:tem=item
            if j>0 and len(item)<self.max_seq_length and len(tem+item)<self.max_seq_length:#and item[-1]=='， O':
                tem=tem+item
            elif(j!=0):
                seqs2.append(tem)
                tem=item
        if tem!='' and tem not in seqs2:#最后一次tmp
            seqs2.append(tem)

        seqs=seqs2
        tem_all=[]
        #print(seqs2)
        seqs_new=[]
        seqs_list_new=[]
        #for i in seqs:
        #    seqs_new.append(i)
            #if len(i)>300 and ',' in i:
        #        seqs_new=seqs_new+[e+d for e in i.split(',') if e]

        for seq_i in seqs:
            # print('seq_i',seq_i)
            #if '公司' not in seq_i:continue
            seqs_list_new.append(list(seq_i))
        #print(2222222222222,seqs)
        if seqs_list_new == []:return []
        rsts = self.encode(seqs_list_new, is_tokenized=True)
        for m,rst in enumerate(rsts):
            # print(rst,seqs_list_new[m])
            tem=''

            for i,k in enumerate(seqs_list_new[m]):
                # print('k',k)
                if i< len(rst)-1:

                    # 针对*ST类数据规则提取
                    if '*' == k and i < len(rst)-4 and seqs_list_new[m][i+1] in ['S','s'] and seqs_list_new[m][i+2] in ['T','t'] and is_Chinese(seqs_list_new[m][i+3]) and is_Chinese(seqs_list_new[m][i+4]):# and (rst[i + 3] in ['B-ORG','I-ORG','E-ORG'] or rst[i + 4] in ['B-ORG','I-ORG','E-ORG'])
                        if i < len(rst)-5 and seqs_list_new[m][i:i+6] in ['A','B'] and '个股' not in ''.join(seqs_list_new[m][i:i+5]) and '公司' not in ''.join(seqs_list_new[m][i:i+5]):
                            tem_all.append(''.join(seqs_list_new[m][i:i+6]))
                        elif '个股' not in ''.join(seqs_list_new[m][i:i+5]) and '公司' not in ''.join(seqs_list_new[m][i:i+5]):
                            tem_all.append(''.join(seqs_list_new[m][i:i+5]))
                        tem=''

                    # 针对ST类数据规则提取
                    if i > 1 and seqs_list_new[m][i-1] != '*' and k in ['S','s'] and i < len(rst)-4 and seqs_list_new[m][i+1] in ['T','t'] and is_Chinese(seqs_list_new[m][i+2]) and is_Chinese(seqs_list_new[m][i+3]):# and (rst[i + 2] in ['B-ORG','I-ORG','E-ORG'] or rst[i + 3] in ['B-ORG','I-ORG','E-ORG'])
                        if i < len(rst)-5 and seqs_list_new[m][i:i+5] in ['A','B'] and '股在' not in ''.join(seqs_list_new[m][i:i+5]) and '公司' not in ''.join(seqs_list_new[m][i:i+5]):
                            tem_all.append(''.join(seqs_list_new[m][i:i+5]))
                        elif '股在' not in ''.join(seqs_list_new[m][i:i+4]) and '公司' not in ''.join(seqs_list_new[m][i:i+4]):
                            tem_all.append(''.join(seqs_list_new[m][i:i+4]))
                        tem=''

                    # 针对S*ST类数据规则提取
                    if k in ['S','s'] and i < len(rst)-6 and seqs_list_new[m][i+1] == '*' and seqs_list_new[m][i+2] in ['S','s'] and seqs_list_new[m][i+3] in ['T','t'] and is_Chinese(seqs_list_new[m][i+4]) and is_Chinese(seqs_list_new[m][i+5]):# and (rst[i + 3] in ['B-ORG','I-ORG','E-ORG'] or rst[i + 4] in ['B-ORG','I-ORG','E-ORG'])
                        if i < len(rst)-7 and seqs_list_new[m][i:i+7] in ['A','B']:
                            tem_all.append(''.join(seqs_list_new[m][i:i+7]))
                        else:
                            tem_all.append(''.join(seqs_list_new[m][i:i+6]))
                        tem=''

                    if rst[i]=='B-ORG' and tem!='':
                        if tem not in tem_all:
                            print(111111111,tem)
                            if tem not in ['集团']:
                                # print(tem)
                                tem_all.append(tem)
                        tem=''
                    if rst[i]=='I-ORG' and tem=='':continue
                    if rst[i]=='E-ORG' and tem=='':continue
                    if rst[i]!='O':
                        #print(k)
                        tem=tem+k
                    if (rst[i]=='B-ORG' and rst[i+1]=='O') or (rst[i]=='I-ORG' and rst[i+1]=='O'):
                        #print(k)
                        tem=''
                    if rst[i]=='E-ORG' and tem!='':
                        #if tem not in tem_all:
                        #print(222222222222,tem)
                        if tem not in ['集团'] and is_Chinese(tem[-2]):
                            # print(tem)
                            tem_all.append(tem)
                        tem=''
        tem_all=list(set(tem_all))
        return tem_all


    @_timeout
    def encode(self, texts, blocking=True, is_tokenized=False):
        """ Encode a list of strings to a list of vectors

        `texts` should be a list of strings, each of which represents a sentence.
        If `is_tokenized` is set to True, then `texts` should be list[list[str]],
        outer list represents sentence and inner list represent tokens in the sentence.
        Note that if `blocking` is set to False, then you need to fetch the result manually afterwards.

        .. highlight:: python
        .. code-block:: python

            with BertClient() as bc:
                # encode untokenized sentences
                bc.encode(['First do it',
                          'then do it right',
                          'then do it better'])

                # encode tokenized sentences
                bc.encode([['First', 'do', 'it'],
                           ['then', 'do', 'it', 'right'],
                           ['then', 'do', 'it', 'better']], is_tokenized=True)

        :type is_tokenized: bool
        :type blocking: bool
        :type timeout: bool
        :type texts: list[str] or list[list[str]]
        :param is_tokenized: whether the input texts is already tokenized
        :param texts: list of sentence to be encoded. Larger list for better efficiency.
        :param blocking: wait until the encoded result is returned from the server. If false, will immediately return.
        :param timeout: throw a timeout error when the encoding takes longer than the predefined timeout.
        :return: encoded sentence/token-level embeddings, rows correspond to sentences
        :rtype: numpy.ndarray or list[list[float]]

        """
        if is_tokenized:
            self._check_input_lst_lst_str(texts)
        else:
            self._check_input_lst_str(texts)

        if self.length_limit and not self._check_length(texts, self.length_limit, is_tokenized):
            warnings.warn('some of your sentences have more tokens than "max_seq_len=%d" set on the server, '
                          'as consequence you may get less-accurate or truncated embeddings.\n'
                          'here is what you can do:\n'
                          '- disable the length-check by create a new "BertClient(check_length=False)" '
                          'when you do not want to display this warning\n'
                          '- or, start a new server with a larger "max_seq_len"' % self.length_limit)

        texts = _unicode(texts)
        # print('texts',texts)
        self._send(jsonapi.dumps(texts), len(texts))
        rst = self._recv_ndarray().content if blocking else None
        # print('rst',rst)
        return rst

    def fetch(self, delay=.0):
        """ Fetch the encoded vectors from server, use it with `encode(blocking=False)`

        Use it after `encode(texts, blocking=False)`. If there is no pending requests, will return None.
        Note that `fetch()` does not preserve the order of the requests! Say you have two non-blocking requests,
        R1 and R2, where R1 with 256 samples, R2 with 1 samples. It could be that R2 returns first.

        To fetch all results in the original sending order, please use `fetch_all(sort=True)`

        :type delay: float
        :param delay: delay in seconds and then run fetcher
        :return: a generator that yields request id and encoded vector in a tuple, where the request id can be used to determine the order
        :rtype: Iterator[tuple(int, numpy.ndarray)]

        """
        time.sleep(delay)
        while self.pending_request:
            yield self._recv_ndarray()

    def fetch_all(self, sort=True, concat=False):
        """ Fetch all encoded vectors from server, use it with `encode(blocking=False)`

        Use it `encode(texts, blocking=False)`. If there is no pending requests, it will return None.

        :type sort: bool
        :type concat: bool
        :param sort: sort results by their request ids. It should be True if you want to preserve the sending order
        :param concat: concatenate all results into one ndarray
        :return: encoded sentence/token-level embeddings in sending order
        :rtype: numpy.ndarray or list[list[float]]

        """
        if self.pending_request:
            tmp = list(self.fetch())
            if sort:
                tmp = sorted(tmp, key=lambda v: v.id)
            tmp = [v.content for v in tmp]
            if concat:
                if self.output_fmt == 'ndarray':
                    tmp = np.concatenate(tmp, axis=0)
                elif self.output_fmt == 'list':
                    tmp = [vv for v in tmp for vv in v]
            return tmp

    def encode_async(self, batch_generator, max_num_batch=None, delay=0.1, is_tokenized=False):
        """ Async encode batches from a generator

        :param is_tokenized: whether batch_generator generates tokenized sentences
        :param delay: delay in seconds and then run fetcher
        :param batch_generator: a generator that yields list[str] or list[list[str]] (for `is_tokenized=True`) every time
        :param max_num_batch: stop after encoding this number of batches
        :return: a generator that yields encoded vectors in ndarray, where the request id can be used to determine the order
        :rtype: Iterator[tuple(int, numpy.ndarray)]

        """

        def run():
            cnt = 0
            for texts in batch_generator:
                self.encode(texts, blocking=False, is_tokenized=is_tokenized)
                cnt += 1
                if max_num_batch and cnt == max_num_batch:
                    break

        t = threading.Thread(target=run)
        t.start()
        return self.fetch(delay)

    @staticmethod
    def _check_length(texts, len_limit, tokenized):
        if tokenized:
            # texts is already tokenized as list of str
            return all(len(t) <= len_limit for t in texts)
        else:
            # do a simple whitespace tokenizer
            return all(len(t.split()) <= len_limit for t in texts)

    @staticmethod
    def _check_input_lst_str(texts):
        if not isinstance(texts, list):
            raise TypeError('"%s" must be %s, but received %s' % (texts, type([]), type(texts)))
        if not len(texts):
            raise ValueError(
                '"%s" must be a non-empty list, but received %s with %d elements' % (texts, type(texts), len(texts)))
        for idx, s in enumerate(texts):
            if not isinstance(s, _str):
                raise TypeError('all elements in the list must be %s, but element %d is %s' % (type(''), idx, type(s)))
            if not s.strip():
                raise ValueError(
                    'all elements in the list must be non-empty string, but element %d is %s' % (idx, repr(s)))

    @staticmethod
    def _check_input_lst_lst_str(texts):
        if not isinstance(texts, list):
            raise TypeError('"texts" must be %s, but received %s' % (type([]), type(texts)))
        if not len(texts):
            raise ValueError(
                '"texts" must be a non-empty list, but received %s with %d elements' % (type(texts), len(texts)))
        for s in texts:
            BertClient._check_input_lst_str(s)

    @staticmethod
    def _force_to_unicode(text):
        # return text if isinstance(text, unicode) else text.decode('utf-8')
        return text.decode('utf-8')

    @staticmethod
    def _print_dict(x, title=None):
        if title:
            print(title)
        for k, v in x.items():
            print('%30s\t=\t%-30s' % (k, v))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class ConcurrentBertClient(BertClient):
    def __init__(self, max_concurrency=10, **kwargs):
        """ A thread-safe client object connected to a BertServer

        Create a BertClient that connects to a BertServer.
        Note, server must be ready at the moment you are calling this function.
        If you are not sure whether the server is ready, then please set `check_version=False` and `check_length=False`

        :type max_concurrency: int
        :param max_concurrency: the maximum number of concurrent connections allowed

        """
        try:
            from bert_base.client import BertClient
        except ImportError:
            raise ImportError('BertClient module is not available, it is required for serving HTTP requests.'
                              'Please use "pip install -U bert-serving-client" to install it.'
                              'If you do not want to use it as an HTTP server, '
                              'then remove "-http_port" from the command line.')

        self.available_bc = [BertClient(**kwargs) for _ in range(max_concurrency)]
        self.max_concurrency = max_concurrency

    def close(self):
        for bc in self.available_bc:
            bc.close()

    def _concurrent(func):
        def arg_wrapper(self, *args, **kwargs):
            try:
                bc = self.available_bc.pop()
                f = getattr(bc, func.__name__)
                r = f if isinstance(f, dict) else f(*args, **kwargs)
                self.available_bc.append(bc)
                return r
            except IndexError:
                raise RuntimeError('Too many concurrent connections!'
                                   'Try to increase the value of "max_concurrency", '
                                   'currently =%d' % self.max_concurrency)

        return arg_wrapper

    @_concurrent
    def encode(self, **kwargs):
        pass

    @property
    @_concurrent
    def server_status(self):
        pass

    @property
    @_concurrent
    def status(self):
        pass

    def fetch(self, **kwargs):
        raise NotImplementedError('Async encoding of "ConcurrentBertClient" is not implemented yet')

    def fetch_all(self, **kwargs):
        raise NotImplementedError('Async encoding of "ConcurrentBertClient" is not implemented yet')

    def encode_async(self, **kwargs):
        raise NotImplementedError('Async encoding of "ConcurrentBertClient" is not implemented yet')
