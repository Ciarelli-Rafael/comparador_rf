import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

from datetime import date
import warnings
warnings.filterwarnings('ignore')

import plotly.graph_objects as go

from pandas.tseries.offsets import CustomBusinessDay
import pandas_market_calendars as mcal

import psycopg2
import locale


def leitura_curva():

    #curva = pd.read_excel("I:\\Guelt AAI\\Tecnologia\\BI e Desenvolvimento\\_PROJETOS\\Projeto Giro rf\\arquivos\\CurvaZero.xlsx")
    
    #curva = curva.drop(0)
    #curva = curva.drop(1)
    #curva = curva.drop(2)
    #curva = curva.drop(3)
    #curva = curva.drop(4)

    #curva.columns = curva.iloc[0]  # Primeira linha se torna o cabeçalho
    #curva = curva[1:]

    #index_8820 = curva[curva['Vertices'] == 8694].index[0]

    # Mantendo apenas as linhas acima ou até o valor 8820
    #curva = curva.loc[:index_8820]
    #curva = curva.dropna(axis=1, how='all')
    #curva['Vertices'] = pd.to_numeric(curva['Vertices'], errors='coerce')
    #curva = curva.ffill()



    connection = psycopg2.connect(
        user="postgres.amebfcyidikmvzqcqqfz",
        password="SEGUROSDATABASE",
        host="aws-0-sa-east-1.pooler.supabase.com",
        port="5432",
        database="postgres"
    )
    connection.autocommit = True
    cursor = connection.cursor()
    plan=cursor.execute(f""" 
        SELECT vertices, ettj_ipca, ettj_pref,inflacao_implicita
        FROM public.curva;
    """)
    plan=cursor.fetchall()

    curva=pd.DataFrame(plan, columns=["Vertices", "ETTJ IPCA", "ETTJ PREF", "Inflação Implícita"])


    #visualização curva transposta
    #df = curva.reset_index()
    df = curva.set_index('Vertices')
    df_t = df.T          
    df_t = df_t.loc[:, df_t.columns <= 3150]
    df_t = df_t.rename_axis('Vertices')



    return curva, df_t

def gera_graf(ativo, financeiro, vencimento, indexador_modi, taxa, curva,ir):
    
    #Como deve chegar os parametros
    
    #financeiro é numero
    #vencimento -> vencimento do papel
    #indexador_modi -> IPCA; %CDI; CDI+; Pré
    #Taxa é número -> tipo: 15.50
    
    ir = ir/100
    
    df = pd.DataFrame()
    #data_inicial = pd.to_datetime('2024-10-04')
    data_inicial = pd.to_datetime('today')
    
    #feriados considerados no intervalo::
    #feriados_br = [data_inicial, vencimento]   # ou pegue de workalendar, etc.
    #cbd = CustomBusinessDay(holidays=feriados_br)
    #dias_uteis = pd.bdate_range(start = data_inicial, end = vencimento,freq = cbd)
    #dias_uteis = dias_uteis.strftime('%d/%m/%Y')
    #df = pd.DataFrame(dias_uteis, columns=['Data'])
    
    #NOVA FORMA PUXANDO DIRETO DA B3
    b3 = mcal.get_calendar('B3')
    agenda  = b3.schedule(start_date=data_inicial, end_date=vencimento)  
    # 2) Pega só as datas (índice)
    dias_uteis = agenda.index            # <-- DatetimeIndex
    # 3) Formata dd/mm/aaaa
    dias_uteis_fmt = dias_uteis.strftime('%d/%m/%Y')
    # 4) DataFrame final
    df = pd.DataFrame(dias_uteis_fmt, columns=['Data'])




    df['dia'] = range(1, len(df) + 1)    
    df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y', errors='coerce')
    
    df['ano'] = df['Data'].dt.year
    
    df['ativo'] = ativo


    df['Financeiro'] = ''
    df.loc[0,'Financeiro'] = financeiro
    df['indexador_modi'] = indexador_modi
    df['dia'] = pd.to_numeric(df['dia'], errors='coerce')
 
    df['Taxa Aplicação Média'] = taxa      
    
    
    
    if (df['indexador_modi'] == 'IPCA').any():
        
        #Aqui pelo arquivo de Anbima:;
        
        df['Mediana'] = pd.merge_asof(
            df.sort_values('dia'),          # DataFrame de referência
            curva.sort_values('Vertices'),         # DataFrame com os intervalos
            left_on='dia', right_on='Vertices',       # Colunas de correspondência
            direction='forward'                      # Associar ao vértice anterior
        )['Inflação Implícita']
        
        #Aqui é pela CVM:: 
        df['taxa_anual'] = df['Taxa Aplicação Média'] + df['Mediana']
        df['taxa_anual'] = pd.to_numeric(df['taxa_anual'], errors='coerce')
        df['taxa_anual'] = df['taxa_anual']/100
        df['taxa_anual'] = df['taxa_anual'] * (1-ir)

       
    elif (df['indexador_modi'] == '%CDI').any():


        df['Mediana'] = pd.merge_asof(
            df.sort_values('dia'),          # DataFrame de referência
            curva.sort_values('Vertices'),         # DataFrame com os intervalos
            left_on='dia', right_on='Vertices',       # Colunas de correspondência
            direction='forward'                      # Associar ao vértice anterior
        )['ETTJ PREF']


    
                            
        #taxa vem com 80% divide por 100 e multiplica depois 
        df['taxa_anual'] = (df['Taxa Aplicação Média']/100) * (df['Mediana']/100)
        df['taxa_anual'] = pd.to_numeric(df['taxa_anual'], errors='coerce')
        df['taxa_anual'] = df['taxa_anual'] * (1-ir)
     
    elif (df['indexador_modi'] == 'CDI+').any():
        

        df['Mediana'] = pd.merge_asof(
            df.sort_values('dia'),          # DataFrame de referência
            curva.sort_values('Vertices'),         # DataFrame com os intervalos
            left_on='dia', right_on='Vertices',       # Colunas de correspondência
            direction='forward'                      # Associar ao vértice anterior
        )['ETTJ PREF']



        
        df['taxa_anual'] = df['Taxa Aplicação Média'] + df['Mediana']
        df['taxa_anual'] = pd.to_numeric(df['taxa_anual'], errors='coerce')
        df['taxa_anual'] = df['taxa_anual']/100
        #calculo taxa liquida
        df['taxa_anual'] = df['taxa_anual'] * (1-ir)

    
    elif (df['indexador_modi'] == 'Pré').any() or (df['indexador_modi'] == 'pré').any():
        
         
              
        #df = pd.merge(df, data_mais_recente[['Indicador', 'DataReferencia', 'Mediana']], left_on=['indexador_modi', 'ano'], right_on=['Indicador', 'DataReferencia'], how='left')
        
        df['Indicador'] = 'Prefixado'
        df['DataReferencia'] =  df['ano']
        
        df['taxa_anual'] = df['Taxa Aplicação Média']
        df['taxa_anual'] = pd.to_numeric(df['taxa_anual'], errors='coerce')
        df['taxa_anual'] = df['taxa_anual']/100
        
        df['Mediana'] =  df['taxa_anual']
        
        #calculo taxa liquida
        df['taxa_anual'] = df['taxa_anual'] * (1-ir)

    else:
         
            
        df = pd.DataFrame()
    
    
    if df.empty:

        do = 'nothing'
        return df

        
        
    else:
        

        #só conseguimos rodar mesmo em dorma de loop         
        for i in range(1, len(df)):
            
             
            # Obter a taxa anual da linha atual
            taxa_anual = df['taxa_anual'].iloc[i]
            
            #(assumindo 252 dias úteis no ano)
            rendimento_diario = (1 + taxa_anual) ** (1 / 252) - 1
            
            df.loc[i, 'Financeiro'] = df.loc[i-1, 'Financeiro'] * (1 + rendimento_diario)
            

        
        return df

def puxa_data(df_ativo,df_ativotroca):
    df_ativotroca = df_ativotroca.rename(columns={'Financeiro':'Financeiro2'})
    df_ativo=pd.merge(df_ativo,df_ativotroca[['dia','Financeiro2']],left_on=['dia'],right_on=['dia'],how='left')
    cond = df_ativo['Financeiro2'] > df_ativo['Financeiro']
    primeira_data = df_ativo.loc[cond, 'Data'].min()
    

    try:

        financeiro_break = df_ativo.loc[df_ativo["Data"] == primeira_data, "Financeiro"].values[0]
    
    except:
        
        financeiro_break = "n tem"
    
    ganho_financeiro = df_ativo['Financeiro2'].iloc[-1] - df_ativo['Financeiro'].iloc[-1]

    return primeira_data, ganho_financeiro,financeiro_break


## APLICATIVO ::

st.set_page_config(
    page_title = "Page",
    page_icon="📈",  # Pode ser um emoji ; caminho para uma imagem
    layout="wide"
)

st.sidebar.title("Developed by Business Intelligence📈")
st.sidebar.markdown("<hr style='border:1px solid #ddd; margin:10px 0;'>", unsafe_allow_html=True)


st.sidebar.title("Parâmetros dos Ativos")

st.sidebar.markdown("### Ativo Atual")
ativo1 = st.sidebar.text_input("Nome do Ativo 1", "NTNB IPCA + 7")
financeiro1 = st.sidebar.number_input("Financeiro (R$) Ativo 1", min_value=0.0, value=1000.0, step=100.0)
vencimento1 = st.sidebar.date_input("Vencimento Ativo 1", value=date(2027, 1, 1))
indexador1 = st.sidebar.selectbox("Indexador Ativo 1", ["%CDI", "CDI+", "Pré", "IPCA"])
taxa1 = st.sidebar.number_input("Taxa (%) Ativo 1", value=80.0, step=0.1)
#agio_desagio = st.sidebar.number_input("Ágio/Deságio (%)", value=0.0, step=0.1,
#                                       help="Use valores negativos para deságio")
ir_1= st.sidebar.number_input("IR Ativo 1", value=0.0, step=0.1,help="Ir se carregado até o vencimento, se for isento deixar 0")

st.sidebar.markdown("---")

st.sidebar.markdown("### Ativo de Troca")
ativo2 = st.sidebar.text_input("Nome do Ativo 2", "NTNB IPCA + 6")
financeiro2 = st.sidebar.number_input("Financeiro (R$) Ativo Troca", min_value=0.0, value=1000.0, step=100.0)
st.sidebar.markdown(f"**Financeiro Ativo 2 (calculado):** R$ {financeiro2:,.2f}")
vencimento2 = st.sidebar.date_input("Vencimento Ativo 2", value=date(2032, 1, 1))
indexador2 = st.sidebar.selectbox("Indexador Ativo 2", ["%CDI", "CDI+", "Pré", "IPCA"], key="indexador2")
taxa2 = st.sidebar.number_input("Taxa (%) Ativo 2", value=84.0, step=0.1)
ir_2= st.sidebar.number_input("IR Ativo 2", value=0.0, step=0.1,help="Ir se carregado até o vencimento, se for isento deixar 0")


#perda_inicial = 

executar = st.sidebar.button("📈 Gerar Gráfico")


# Opção ativa do menu

# Estilo para diferenciar o item ativo
st.markdown(
    f"""
    <style>
       {{
            background-color: #FF4B4B;
            color: white;
            border-radius: 5px;
            padding: 5px 10px;
            font-weight: bold;
        }}
          
    </style>
    """,
    unsafe_allow_html=True,
)

#pagina principal

    # Título do aplicativo
st.title("Oportunidade de Alongamento")
st.markdown("<br>", unsafe_allow_html=True)

    # Subtítulo
#st.subheader("Comparador de Ativos de Renda Fixa")
curva, df_t = leitura_curva()

if executar:


    df_ativo = gera_graf(ativo1, financeiro1, vencimento1, indexador1, taxa1, curva, ir_1)
    df_ativo_troca= gera_graf(ativo2, financeiro2, vencimento2, indexador2, taxa2, curva, ir_2)
        
    grafico_final = pd.concat([df_ativo, df_ativo_troca], ignore_index=True)
    data_break,ganho_financeiro,financeiro_break = puxa_data(df_ativo,df_ativo_troca)
    ganho_financeiro = int(ganho_financeiro)

    #df_ativo.to_excel("ativo1.xlsx")
    #df_ativo_troca.to_excel("ativotroca.xlsx")
    
    try:

        data_breakk = data_break.strftime('%d/%m/%Y')
    except:
        data_breakk="--"


    data_venc1 = vencimento1.strftime('%d/%m/%Y')
    data_venc2 = vencimento2.strftime('%d/%m/%Y')


    if not grafico_final.empty:

            
            
        col1, col2, col3, col4 = st.columns(4)

        #col1, col2 = st.columns(2)
        #col3, col4 = st.columns(2)



        card_style = """
            background-color: white;
            padding: 10px;
            border-radius: 10px;
            border: 1px solid #ddd;
            box-shadow: 2px 2px 6px rgba(0,0,0,0.05);
            text-align: center;
            font-size: 14px;
            width: 90%;
            margin: auto;
        """


        with col1:   #caso queria deixar normal, sem ser centralizado, é so tirar o text-align
            st.markdown(f"""
                <div style="{card_style}"> 
                    <h4>Posição Atual</h4>
                    <h5>{ativo1} - {data_venc1}</h5>
                </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown(f"""
                <div style="{card_style}">
                    <h4>Ativo de Troca</h4>
                    <h5>{ativo2} - {data_venc2}</h5>
                </div>
            """, unsafe_allow_html=True)

        with col3:
            st.markdown(f"""
                <div style="{card_style}">
                    <h4>Data Break Even</h4>
                    <h5>{data_breakk}</h5>
                </div>
            """, unsafe_allow_html=True)

        with col4:
            st.markdown(f"""
                <div style="{card_style}">
                    <h4>Ganho Financeiro</h4>
                    <h5>R$ {ganho_financeiro}</h5>
                </div>
            """, unsafe_allow_html=True)        


        # 1️⃣  Mapeie cada ativo para a cor desejada
        color_map = {
            ativo1: "#4DA6FF",  # azul claro
            ativo2: "#66CC99",  # verde claro
        }
        # 2️⃣  Crie o gráfico já indicando o mapa de cores
        fig = px.line(
            grafico_final,
            x="Data",
            y="Financeiro",
            color="ativo",
            hover_data=["Data", "Financeiro"],  # ou outras colunas

            title="Gráfico de Rentabilidade Comparada",
            labels={"Data": "Data", "Financeiro": "Valor Financeiro"},
            color_discrete_map=color_map,
            line_shape="spline"                 # ← curva suave
          # 👈 força azul/verde
        )



        # 3️⃣  Deixe as linhas mais grossas (ex.: 3 px)
        #fig.update_traces(line=dict(width=3))     # afeta todas as séries
        fig.update_traces(line=dict(shape="spline", width=2.7))

        # 4️⃣  Ajustes de layout
        fig.update_layout(
            xaxis_title="Data",
            yaxis_title="Valor Financeiro",
            width=2000,
            height=800
        )

        #taxa break 
        fig.add_trace(go.Scatter(
            x=[data_break],
            y=[financeiro_break],
            mode="markers+text",
            marker=dict(color="red", size=8),
            text=["Break Even"],
            textposition="top center",
            showlegend=False
        ))


        locale.setlocale(locale.LC_ALL, '')
        ultimos = grafico_final.sort_values("Data").groupby("ativo").tail(1)

        for i, row in ultimos.iterrows():


            try:
                valor_fmt = locale.currency(row["Financeiro"], symbol='R$', grouping=True)
            except Exception:
                valor_fmt = f'R$ {row["Financeiro"]:,.2f}'.replace(',', 'v').replace('.', ',').replace('v', '.')



            fig.add_scatter(
                x=[row["Data"]],
                y=[row["Financeiro"]],
                mode="markers+text",
                marker=dict(size=8, color=color_map.get(row["ativo"], "#000")),
                #text=[valor_fmt],
                text=[f"<b>{valor_fmt}</b>"],
                textposition="middle right",
                showlegend=False
            )




        # 5️⃣  Exibe no Streamlit
        st.plotly_chart(fig)
        st.info("ℹ️ O gráfico apresenta a comparação de rentabilidade líquida de imposto de renda dos ativos.")


        st.markdown("""
        **Disclaimers:**

        1) Os fundos de investimento referidos neste e-mail podem utilizar estratégias com derivativos como parte integrante de suas políticas de investimento.  
        2) Não há garantia de que os fundos de investimento referidos neste e-mail terão o tratamento tributário de fundos de longo prazo.  
        3) Os fundos de investimento referidos neste e-mail podem estar expostos a diferentes tipos de risco, tais como: crédito, mercado, liquidez operacional, etc.  
        4) **RECOMENDAMOS SEMPRE A LEITURA INTEGRAL DO REGULAMENTO DOS FUNDOS DE INVESTIMENTO, BEM COMO DE TODO E QUALQUER DOCUMENTOS CONTENDO INFORMAÇÕES RELEVANTES E ESSENCIAIS SOBRE OS REFERIDOS FUNDOS, ANTES DE REALIZAR QUALQUER INVESTIMENTO.**  
        5) Para avaliação da performance de um fundo de investimento, é recomendável a análise de, no mínimo, um período composto de 12 (doze) meses.  
        6) **RENTABILIDADE PASSADA NÃO REPRESENTA GARANTIA DE RENTABILIDADE FUTURA. FUNDOS DE INVESTIMENTO NÃO CONTAM COM GARANTIA DO ADMINISTRADOR, DO GESTOR, DE QUALQUER MECANISMO DE SEGURO OU FUNDO GARANTIDOR DE CRÉDITO – FGC.** Assim, não é possível prever o desempenho futuro de um investimento a partir da variação de seu valor de mercado no passado.  
        7) As informações contidas neste e-mail não podem ser consideradas como única fonte de informações no processo decisório do cliente, que, antes de tomar qualquer decisão, deverá realizar uma avaliação minuciosa do produto e respectivos riscos, face aos seus objetivos pessoais e ao seu perfil de risco.  
        8) Este relatório foi preparado com o objetivo de simples conferência, as informações e saldos estão sujeitos à confirmação. Este material não tem caráter técnico ou publicitário, apenas informativo. Da mesma forma, embora as informações tenham sido obtidas de fontes confiáveis e fidedignas, nenhuma garantia ou responsabilidade, expressa ou implícita, é feita a respeito da exatidão, fidelidade e/ou totalidade das informações.  
        9) Este relatório contém um breve resumo de cunho meramente informativo, não configurando análise de valores mobiliários nos termos da Instrução CVM Nº 598, de 03 de maio de 2018, e não tendo como objetivo a consultoria, oferta, solicitação de oferta, ou recomendação para a compra ou venda de qualquer investimento ou produto específico.  
        10) O presente relatório foi elaborado pela Guelt AAI, com base nas informações transmitidas pelo Administrador e pelo cliente, e não tem por objetivo substituir o extrato contendo informações sobre as operações realizadas ou posições em aberto do cliente.  
        11) Os sócios/atendentes da Guelt AAI estão identificados no site www.gueltinvestimentos.com.br
        """)


        #st.subheader("Projeção Anbima")
        #st.dataframe(df_t)
    
   
    
    
    else:
        st.warning("Não há dados suficientes para gerar o gráfico.")

else:
    st.info("Preencha os dados no menu lateral e clique em **Gerar Gráfico**.")
    st.subheader("Projeção Anbima")
    st.dataframe(df_t)
