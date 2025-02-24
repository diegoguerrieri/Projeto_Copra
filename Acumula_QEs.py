import pandas as pd
import glob

def Import379(diretorio):

    nomes = ["EBISEQ", "ENTCODIGO", "MRFMESANO", "QUAID", "TPMOID", "TPMOPREV", "CMPID", "PLNCODIGO", "EBINUMPROP", 
             "EBICPFPART", "EBICPFBENF", "EBIDATAINICIO", "EBIDATAFIM", "EBIDATAOCORR", "EBIDATAREG", "EBIDATACOMUNICA", 
             "EBIBENVEN", "EBIVALORPBAC", "EBIVALORPBC", "EBIVALORMOV", "EBIVALORMON", "EBICODCESS", "EBINUMSIN"]
    tipo = {"EBISEQ": int, "ENTCODIGO": int, "MRFMESANO": int, "QUAID": int, "TPMOID": int, "TPMOPREV": int, 
            "CMPID": int, "PLNCODIGO": int, "EBINUMPROP": str, "EBICPFPART": str, "EBICPFBENF": str, "EBIDATAINICIO": int, 
            "EBIDATAFIM": int, "EBIDATAOCORR": int, "EBIDATAREG": int, "EBIDATACOMUNICA":int, "EBIBENVEN": int, 
            "EBIVALORPBAC": float, "EBIVALORPBC": float, "EBIVALORMOV": float, "EBIVALORMON": float, "EBICODCESS": int, 
            "EBINUMSIN": str}
    seq = [7, 5, 8, 3, 4, 4, 4, 6, 21, 11, 11, 8, 8, 8, 8, 8, 4, 13, 13, 13, 13, 5, 20]

    files = glob.glob(str.format(f"{diretorio}/EST379_*.txt"))
    ret = []
    
    for i in files:
        df = pd.read_fwf(i, 
                         widths=seq, 
                         header=None, 
                         names=nomes, 
                         dtype=tipo, 
                         decimal=",")  
        # Agora convertemos manualmente as colunas de data
        campos_datas = ["MRFMESANO","EBIDATAINICIO", "EBIDATAFIM", "EBIDATAOCORR", "EBIDATAREG", "EBIDATACOMUNICA"]
        for campo in campos_datas:
            df[campo] = pd.to_datetime(df[campo], format='%Y%m%d', errors='coerce')  # Convertendo para datetime

        ret.append(df)

    final = pd.concat(ret, ignore_index=True)
    
    return final

def Import380(diretorio):

    nomes = ["EBISEQ", "ENTCODIGO", "MRFMESANO", "QUAID", "CMPID", "PLNCODIGO", "EBRNUMPROP","EBRCPFPART", "EBRCPFBENF", 
             "EBRDATAINICIO", "EBRDATAFIM", "EBRDATAOCORR", "EBRDATAREG", "EBRVALORMOV", "EBRCODCESS", "EBRNUMSIN"]
    tipo = {"EBRSEQ": int, "ENTCODIGO": int, "MRFMESANO": int, "QUAID": int, "CMPID": int, "PLNCODIGO": int, 
            "EBRNUMPROP": str, "EBRCPFPART": str, "EBRCPFBENF": str, "EBRDATAINICIO": int, "EBRDATAFIM": int, 
            "EBRDATAOCORR": int, "EBRDATAREG": int, "EBRVALORMOV": float, "EBRCODCESS": int, "EBRNUMSIN": str}
    seq = [7, 5, 8, 3, 4, 6, 21, 11, 11, 8, 8, 8, 8, 13, 5, 20]

    files = glob.glob(str.format(f"{diretorio}/EST380_*.txt"))
    ret = []

    for i in files:
        df = pd.read_fwf(i, 
                          widths=seq, 
                          header=None, 
                          names=nomes, 
                          dtype=tipo, 
                          decimal=",")
        # Agora convertemos manualmente as colunas de data
        campos_datas = ["MRFMESANO","EBRDATAINICIO", "EBRDATAFIM", "EBRDATAOCORR", "EBRDATAREG"]
        for campo in campos_datas:
            df[campo] = pd.to_datetime(df[campo], format='%Y%m%d', errors='coerce')  # Convertendo para datetime

        ret.append(df)

    final = pd.concat(ret, ignore_index=True)

    return final

def Import376(diretorio):

    nomes = ["ESRSEQ", "ENTCODIGO", "MRFMESANO", "QUAID", "TPMOID", "CMPID", "RAMCODIGO", "ESRDATAINICIO", "ESRDATAFIM", 
             "ESRDATAOCORR", "ESRDATAREG", "ESRVALORMOV", "ESRDATACOMUNICA", "ESRCODCESS", "ESRNUMSIN", "ESRVALORMON"]
    tipo = {"ESRSEQ": int, "ENTCODIGO": int, "MRFMESANO": int, "QUAID": int, "TPMOID": int, "CMPID": int, "RAMCODIGO": int, 
            "ESRDATAINICIO": int, "ESRDATAFIM": int, "ESRDATAOCORR": int, "ESRDATAREG": int, "ESRDATACOMUNICA": int, 
            "ESRVALORMOV": float, "ESRCODCESS": int, "ESRNUMSIN": str, "ESRVALORMON": float}
    seq = [7, 5, 8, 3, 4, 4, 4, 8, 8, 8, 8, 13, 8, 5, 20, 13]

    files = glob.glob(str.format(f"{diretorio}/EST376_*.txt"))
    ret = []

    for i in files:
        df = pd.read_fwf(i, 
                         widths=seq, 
                         header=None, 
                         names=nomes, 
                         dtype=tipo, 
                         decimal=",")
        # Agora convertemos manualmente as colunas de data
        campos_datas = ["MRFMESANO","ESRDATAINICIO", "ESRDATAFIM", "ESRDATAOCORR", "ESRDATAREG", "ESRDATACOMUNICA"]
        for campo in campos_datas:
            df[campo] = pd.to_datetime(df[campo], format='%Y%m%d', errors='coerce')  # Convertendo para datetime

        ret.append(df)

    final = pd.concat(ret, ignore_index=True)

    return final

def Import377(diretorio):

    nomes = ["ESLSEQ", "ENTCODIGO", "MRFMESANO", "QUAID", "CMPID", "RAMCODIGO", "ESLDATAINICIO", "ESLDATAFIM", "ESLDATAOCORR", 
             "ESLDATAREG", "ESLVALORMOV", "ESLCODCESS", "ESLNUMSIN"]
    tipo = {"ESLSEQ": int, "ENTCODIGO": int, "MRFMESANO": int, "QUAID": int, "CMPID": int, "RAMCODIGO": int, 
            "ESLDATAINICIO": int, "ESLDATAFIM": int, "ESLDATAOCORR": int, "ESLDATAREG": int, "ESLDATACOMUNICA": int, 
            "ESLVALORMOV": float, "ESLCODCESS": int, "ESLNUMSIN": str}
    seq = [7, 5, 8, 3, 4, 4, 8, 8, 8, 8, 13, 5, 20]

    files = glob.glob(str.format(f"{diretorio}/EST377_*.txt"))
    ret = []

    for i in files:
        df = pd.read_fwf(i, 
                         widths=seq, 
                         header=None, 
                         names=nomes, 
                         dtype=tipo, 
                         decimal=",")
         # Agora convertemos manualmente as colunas de data
        campos_datas = ["MRFMESANO","ESLDATAINICIO", "ESLDATAFIM", "ESLDATAOCORR", "ESLDATAREG"]
        for campo in campos_datas:
            df[campo] = pd.to_datetime(df[campo], format='%Y%m%d', errors='coerce')  # Convertendo para datetime

        ret.append(df)

    final = pd.concat(ret, ignore_index=True)

    return final

def Import378(diretorio):
    nomes = ["ESPSEQ", "ENTCODIGO", "MRFMESANO", "QUAID", "TPMOID", "CMPID", "RAMCODIGO", "ESPDATAINICIORO", "ESPDATAFIMRO", "ESPDATAEMISSRO",
             "ESPVALORMOVRO", "ESPDATAINICIORD", "ESPDATAFIMRD", "ESPDATAEMISSRD", "ESPVALORMOVRD", "ESPCODCESS", "ESPFREQ", "ESPVALORCARO", "ESPVALORCARD",
             "ESPVALORCIRO", "ESPVALORCIRD", "ESPMOEDA"]
    tipo = {"ESPSEQ": int, "ENTCODIGO": int, "MRFMESANO": int, "QUAID": int, "TPMOID": str, "CMPID": str, "RAMCODIGO": str, "ESPDATAINICIORO": int, "ESPDATAFIMRO": int, "ESPDATAEMISSRO": int,
            "ESPVALORMOVRO": float, "ESPDATAINICIORD": str, "ESPDATAFIMRD": str, "ESPDATAEMISSRD": str, "ESPVALORMOVRD": float, "ESPCODCESS": int, "ESPFREQ": int, "ESPVALORCARO": float,
            "ESPVALORCARD": float, "ESPVALORCIRO": float, "ESPVALORCIRD": float, "ESPMOEDA": int}
    seq = [7, 5, 8, 3, 4, 4, 4, 8, 8, 8, 13, 8, 8, 8, 13, 5, 4, 13, 13, 13, 13, 3]
    files = glob.glob(str.format(f"{diretorio}/EST378_*.txt"))
    ret = []

    for i in files:
        df = pd.read_fwf(i, 
                         widths=seq, 
                         header=None, 
                         names=nomes, 
                         dtype=tipo, 
                         decimal=",")
         # Agora convertemos manualmente as colunas de data
        campos_datas = ["MRFMESANO","ESPDATAINICIORO", "ESPDATAFIMRO", "ESPDATAEMISSRO", "ESPDATAINICIORD", 
                        "ESPDATAFIMRD", "ESPDATAEMISSRD"]
        for campo in campos_datas:
            df[campo] = pd.to_datetime(df[campo], format='%Y%m%d', errors='coerce')  # Convertendo para datetime

        ret.append(df)

    final = pd.concat(ret, ignore_index=True)

    return final

def Import382(diretorio):
    nomes = ["ESCSEQ", "ENTCODIGO", "MRFMESANO", "QUAID", "TPMOID", "CMPID", "PLNCODIGO", "ESCDATAINICIORO", "ESCDATAFIMRO", "ESCDATAEMISSRO",
             "ESCVALORMOVRO", "ESCDATAINICIORD", "ESCDATAFIMRD", "ESCDATAEMISSRD", "ESCVALORMOVRD", "ESCIMPSEG", "ESPCODCESS", "ESCFREQ", "ESCVALORCARO", "ESCVALORCARD",
             "ESCVALORCIRO", "ESCVALORCIRD"]
    tipo = {"ESCSEQ": int, "ENTCODIGO": int, "MRFMESANO": int, "QUAID": int, "TPMOID": str, "CMPID": str, "PLNCODIGO": str, "ESCDATAINICIORO": int, "ESCDATAFIMRO": int, "ESCDATAEMISSRO": int,
            "ESCVALORMOVRO": float, "ESCDATAINICIORD": str, "ESCDATAFIMRD": str, "ESCDATAEMISSRD": str, "ESCVALORMOVRD": float, "ESCIMPSEG": int, "ESPCODCESS": int, "ESCFREQ": int, "ESCVALORCARO": float,
            "ESCVALORCARD": float, "ESCVALORCIRO": float, "ESCVALORCIRD": float}
    seq = [7, 5, 8, 3, 4, 4, 6, 8, 8, 8, 13,
           8, 8, 8, 13, 16, 5, 4, 13, 13, 13, 13]
    files = glob.glob(str.format(f"{diretorio}/EST382_*.txt"))
    ret = []

    for i in files:
        df = pd.read_fwf(i, 
                         widths=seq, 
                         header=None, 
                         names=nomes, 
                         dtype=tipo, 
                         decimal=",")
         # Agora convertemos manualmente as colunas de data
        campos_datas = ["MRFMESANO","ESCDATAINICIORO", "ESCDATAFIMRO", "ESCDATAEMISSRO", "ESCDATAINICIORD", 
                        "ESCDATAFIMRD", "ESCDATAEMISSRD"]
        for campo in campos_datas:
            df[campo] = pd.to_datetime(df[campo], format='%Y%m%d', errors='coerce')  # Convertendo para datetime

        ret.append(df)

    final = pd.concat(ret, ignore_index=True)

    return final

