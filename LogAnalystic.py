#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 作成バージョン: python 3.6+
# 依存ライブラリ: PyYAML, Pandas, XlsxWriter, Numpy

login_kbn_dic = {
    1:'ＰＣ版',
    2:'携帯版',
    6:'Ｐフラッシュ',
    7:'スマホアプリ版',
    8:'スマホWEB版',
    9:'委託者タブレット' }

# 分析データ格納用
summary = {}

def prg():
    sys.stdout.write('.')
    sys.stdout.flush()

def load_config():
    import yaml
    basedir = os.path.split(__file__)[0]
    f = open(os.path.join(basedir, 'LogAnalystic.config.yaml'), 'r')
    global config
    config = yaml.load(f)
    f.close()
    import calendar
    global target_yearmonth, target_yearmonth_start, target_yearmonth_end
    ty = datetime.datetime.strptime(config['target_yearmonth'], '%Y%m').date()
    target_yearmonth = ty
    target_yearmonth_start = datetime.date(ty.year, ty.month, 1)
    target_yearmonth_end = target_yearmonth_start + datetime.timedelta(days = calendar.monthrange(ty.year, ty.month)[1] - 1)

def setup_log():
    from logging import getLogger, StreamHandler, FileHandler, Formatter, DEBUG
    global logger
    logger = getLogger(__name__)
    logfmt = Formatter('%(asctime)s - %(message)s')
    stdHandler = StreamHandler()
    stdHandler.setLevel(DEBUG)
    stdHandler.setFormatter(logfmt)
    logger.setLevel(DEBUG)
    logger.addHandler(stdHandler)

    fileHandler = FileHandler(config['log_file'], 'a+')
    fileHandler.setFormatter(logfmt)
    logger.addHandler(fileHandler)
    logger.propagate = False

def get_tmpfile(file_index = 0):
    tmp_dir = os.environ['TMP']
    return os.path.join(tmp_dir, 'LogAnalystic_' + str(runtime_timestamp) + str(file_index) + '.dat')

def get_outputfile():
    return config['output_file'].format(target_yearmonth=config['target_yearmonth'])

class FileStore:
    files = [] # ファイルリスト

    #ファイルを連結する
    def joint_logs(self, input_dir):
        logger.info('  ファイルの連結処理を開始します。')
        keyword = config['formatter']['extract_keyword']

        # ファイルを探す
        for name, path in input_dir.items():
            self.find_file(path)

        count = 0
        total = len(self.files)
        with open(get_tmpfile(), 'a+') as w:
            # ファイルからデータを抽出して結合
            for file_path in self.files:
                with open(file_path, 'r') as f:
                    for line in f:
                        if line.find(keyword) >= 0:
                            w.write(line)
                            # 改行が無ければ追記
                            if not line.endswith('\n'):
                                w.write('\n')
                count += 1
                sys.stdout.write('\r結合したファイル: {c}'.format(c=count))
                sys.stdout.flush()
                ############## デバッグ ################
                #break
        sys.stdout.write('\r')
        logger.info('  {c}件のファイル連結処理が終了しました。'.format(c=count))
        return get_tmpfile()

    def find_file(self, input_dir):
        if os.path.isfile(input_dir):
            self.files.append(input_dir)
        elif os.path.isdir(input_dir):
            dir_list = os.listdir(input_dir)
            for inner_dir in dir_list:
                self.find_file(os.path.join(input_dir, inner_dir))

def make_dataframe(joined_file):
    data_list = []
    count = 0
    # ログデータを指定された桁数で読み取る
    logger.info('  データを集計しています...')
    definition_dic = config['formatter']['data_definition']
    #   文字の切り出しに最低限必要な長さを計算
    min_len = max([dd for dd in definition_dic.values() if 'start' in dd and 'length' in dd], key=lambda x: x['start'] + x['length'])
    min_len = min_len['start'] + min_len['length']
    #   ファイル読み込み
    with open(joined_file, "r") as r:
        for line in r:
            if len(line) < min_len:
                continue
            splitted = split_line(line)
            if splitted == None:
                continue
            data_list.append(splitted)
            count += 1
            if count % 1000 == 1:
                sys.stdout.write('\r件数: {c}'.format(c=count))
                sys.stdout.flush()
    sys.stdout.write('\r')
    logger.info('  {c}件のデータ集計が完了しました。'.format(c=count))
    # データフレームを作成
    data_frame = pd.DataFrame(data_list)
    # ヘッダーを作成
    data_frame.columns = [dd['header'] for dd in config['formatter']['data_definition'].values()]
    logger.info("  ソート処理を実行しています...")
    # ソート
    #   'sort'項目があるデータ定義を収集
    sortable_dds = [dd for dd in config['formatter']['data_definition'].values() if 'sort' in dd]
    #   'sort'でデータ定義をソート
    sorted(sortable_dds, key=lambda dd: dd['sort'])
    sort_header_names = [dd['header'] for dd in sortable_dds]
    #   データフレームにソートするヘッダー名を指定してソート
    data_frame = data_frame.sort_values(sort_header_names, ascending=True)
    return data_frame

# ログファイルの行をデータへ分割する
def split_line(line):
    dic = {}
    for name, definition in config['formatter']['data_definition'].items():
        # 'start'が存在する場合は文字を切り出す
        if 'start' in definition and 'length' in definition:
            start, length = definition['start'] - 1, definition['length']
            _data_ = line[start : start + length]
            # データ変換方式convertが指定されている場合はコードとして評価
            if 'convert' in definition:
                _data_ = eval(definition['convert'])
        else:
            _data_ = None
            # 'reference'が存在する場合は既に切り出したデータを参照する
            if 'reference' in definition and definition['reference'] in dic:
                _data_ = dic[definition['reference']]
            # 'operation'が存在する場合はその文字列を実行コードとして評価
            if 'operation' in definition:
                _data_ = eval(definition['operation'])
        # 'filter'が存在する場合は関数を実行し、Falseの場合はデータを除外する
        if 'filter' in definition:
            if not globals()[definition['filter']](_data_):
                return None
        dic[name] = _data_
    return [v for v in dic.values()]


# 対象月だけでフィルターする用の関数
def depends_on_target_yearmonth(date):
    return target_yearmonth_start <= date and date <= target_yearmonth_end


class ExcelHandler:
    def __init__(self, output_file):
        # XlsxWriterをExcelWriterとして指定
        # 日付データ型に対するフォーマットも指定する
        self.excel_writer = pd.ExcelWriter(
                output_file,
                engine='xlsxwriter',
                datetime_format='hh:mm:ss',
                date_format='yyyy/mm/dd')
        self.filename = output_file

    # Excelへの書き込み
    def write_excel(self, data_frame, sheet_name, *, index=True):
        if isinstance(data_frame, list):
            row_index = 1
            for df in data_frame:
                df.to_excel(self.excel_writer, sheet_name=sheet_name, index=index, startrow=row_index)
                row_index += len(df) + 2 # 行数+ヘッダ1行+マージン1行
        else:
            data_frame.to_excel(self.excel_writer, sheet_name=sheet_name, index=index)

    def save(self):
        self.excel_writer.save()

    def cross_by_user(self, data_frame):
        # 処理用データを作成
        rawdata_df = pd.crosstab(data_frame['委託者コード'], data_frame['ログイン区分'])
        # 表示用データを作成
        dispdata_df = rawdata_df.copy()
        dispdata_df['合計'] = rawdata_df.sum(axis=1)
        statistics_df = pd.DataFrame(dispdata_df.sum(axis=0), columns=['ログイン回数']).T

        # 総計の百分率を作成
        total_rates = self.calc_rate(list(rawdata_df.sum()))
        total_rates.append(numpy.nan)
        total_rates_df = pd.DataFrame([total_rates], columns=statistics_df.columns, index=['ログイン回数(％)'])
        statistics_df = statistics_df.append(total_rates_df)
        

        # 各チャネル利用委託者数を計算
        # PC, 携帯, ..., 総計
        available_columns = [x for x in rawdata_df.columns.values if self.show_login_kbn(x) != None]
        count_row = [len(rawdata_df[ rawdata_df[x] > 0]) for x in available_columns]
        count_rates = self.calc_rate(count_row)
        # 合計列追加
        count_row.append(len(rawdata_df)) #ログインのあった委託者総数
        count_rates.append(numpy.nan) # NaN
        user_by_channel_df = pd.DataFrame([count_row, count_rates],
                columns=statistics_df.columns, index=['各チャネル利用委託者数','各チャネル利用委託者数(％)'])
        statistics_df = statistics_df.append(user_by_channel_df)


        # 最大ログイン回数を算出
        login_frequency = list(rawdata_df.max())
        # 百分率で割合を計算 (小数点1桁で丸め)
        login_frequency_rates = self.calc_rate(login_frequency)
        # 合計列追加
        login_frequency.append(rawdata_df.sum(axis=1).max())
        login_frequency_rates.append(numpy.nan) # NaN
        login_frequency_df = pd.DataFrame([login_frequency, login_frequency_rates],
                columns=statistics_df.columns, index=['１委託者最大ログイン数','１委託者最大ログイン数(％)'])
        statistics_df = statistics_df.append(login_frequency_df)

        # 最大ログイン回数を叩きだしているユーザーを算出
        statistics_df = statistics_df.append(pd.DataFrame(dispdata_df.idxmax(), columns=["最大ログイン数(委託者コード)"]).T)
        
        # 列名をリネーム
        dispdata_df = self.rename_login_kbn(dispdata_df)
        statistics_df = self.rename_login_kbn(statistics_df)
        dispdata_df = dispdata_df.append(statistics_df)
        return (statistics_df, dispdata_df)

    # 日付別に集計
    def cross_by_date(self, data_frame, total_login):
        rawdata_df = pd.crosstab(data_frame['日付'], data_frame['ログイン区分'])
        rawdata_df = self.rename_login_kbn(rawdata_df)
        rawdata_df['合計'] = rawdata_df.sum(axis=1)
        rawdata_df['合計(％)'] = round(rawdata_df['合計'] / total_login * 100, 1)
        return rawdata_df

    # 時刻別に集計
    def cross_by_hour(self, data_frame, total_login):
        rawdata_df = pd.crosstab(data_frame['時'], data_frame['ログイン区分'])
        rawdata_df = self.rename_login_kbn(rawdata_df)
        rawdata_df['合計'] = rawdata_df.sum(axis=1)
        rawdata_df['合計(％)'] = round(rawdata_df['合計'] / total_login * 100, 1)
        return rawdata_df

    # 曜日別に集計
    def cross_by_weekday(self, data_frame, total_login):
        rawdata_df = pd.crosstab(data_frame['曜日'], data_frame['ログイン区分'])
        rawdata_df = self.rename_login_kbn(rawdata_df)
        rawdata_df['合計'] = rawdata_df.sum(axis=1)
        rawdata_df['合計(％)'] = round(rawdata_df['合計'] / total_login * 100, 1)
        # 日月火...で並び替え
        rawdata_df = pd.concat([rawdata_df.ix[[w]] for w in '日月火水木金土'])
        return rawdata_df

    def calc_rate(self, target_list):
        sumv = sum(target_list)
        return [round(x / sumv * 100, 1) for x in target_list]

    def rename_login_kbn(self, data_frame):
        colmap = {k: self.show_login_kbn(k) for k in data_frame.columns.values if self.show_login_kbn(k) != None}
        return data_frame.rename(columns = colmap)

    def show_login_kbn(self, login_kbn):
        if login_kbn in login_kbn_dic:
            return login_kbn_dic[login_kbn]
        else:
            return None


# 分析処理
def analyze():
    try:
        logger.info(u'ログ分析処理を開始します。')
        fs = FileStore()
        joined_file = fs.joint_logs(config['input_directory'])
        master_data = make_dataframe(joined_file)
        summary['masterdata'] = master_data
        os.remove(joined_file) # 一時ファイルを削除

        # Excel処理
        logger.info('  ファイル[{f}]を書き込んでいます...'.format(f=get_outputfile()))
        eh = ExcelHandler(get_outputfile())
        eh.write_excel(master_data, 'マスターデータ', index=False)

        summary['statistic'], summary['user'] = eh.cross_by_user(master_data)
        eh.write_excel(summary['user'], '委託者別')
        total_login = summary['statistic']['合計']['ログイン回数']

        summary['date'] = eh.cross_by_date(master_data, total_login)
        eh.write_excel(summary['date'], '日付別')

        summary['weekday'] = eh.cross_by_weekday(master_data, total_login)
        eh.write_excel(summary['weekday'], '曜日')

        summary['hour'] = eh.cross_by_hour(master_data, total_login)
        eh.write_excel(summary['hour'], '時刻別')

        eh.write_excel(
                [summary['statistic'], summary['date'], summary['weekday'], summary['hour']], '統計')

        eh.save()
        logger.info(u'ログ分析処理を終了します。')
    except Exception as err:
        logger.exception('例外が発生しました: %s', err)
        logger.info(u'ログ分析処理は途中で終了しました。')

def talk():
    fs = FileStore()
    joined_file = fs.joint_logs(config['input_directory'])
    master_data = make_dataframe(joined_file)
    summary['masterdata'] = master_data
    os.remove(joined_file) # 一時ファイルを削除
    eh = ExcelHandler(get_outputfile())
    summary['statistic'], summary['user'] = eh.cross_by_user(master_data)
    total_login = summary['statistic']['合計']['ログイン回数']
    summary['date'] = eh.cross_by_date(master_data, total_login)
    summary['weekday'] = eh.cross_by_weekday(master_data, total_login)
    summary['hour'] = eh.cross_by_hour(master_data, total_login)


# ここからメイン処理
print(u'---初期化中---')
import os, sys
prg()
import time, datetime
prg()
import numpy
prg()
import pandas as pd
prg()
runtime_timestamp = time.time()
prg()
load_config()
prg()
setup_log()
prg()
print(u'\n---初期化終了---')

analyze()


