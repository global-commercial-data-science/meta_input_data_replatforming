import pandas as pd
import numpy as np
from tqdm import tqdm
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import MinMaxScaler
pd.options.mode.chained_assignment = None

# Function to format the dataset pivoted with sql
def column_name_formatting(df):

    formatted_df = df.copy()
    new_col_names = []
    for cols in df.columns.to_list():
        new_name = cols.replace("'",'')
        new_col_names.append(new_name)

    formatted_df.columns = new_col_names

    return formatted_df

#-----------------------------------------------------------------------------------------------------------------------
# Function to remove outliers based on total listen time
def remove_outliers(df):

    df_without_max = df.drop(df['TOTAL_LISTEN_TIME'].idxmax())
    Q1 = df_without_max['TOTAL_LISTEN_TIME'].quantile(0.25)
    Q3 = df_without_max['TOTAL_LISTEN_TIME'].quantile(0.95)
    IQR = Q3 - Q1
    df_outlier_removed = df_without_max[~pd.DataFrame((df_without_max['TOTAL_LISTEN_TIME'] < (Q1)) |
                                                      (df_without_max['TOTAL_LISTEN_TIME'] > (Q3 + 1.5 * IQR))).any(axis=1)]
    print(f'Q1: {Q1}, Q3: {Q3}, UL: {Q3 + 1.5 * IQR}, LL: {Q3 - 1.5 * IQR}')

    return df_outlier_removed

#-----------------------------------------------------------------------------------------------------------------------
# Function to group Itunes Categories hierarchically to tune segment prediction

def group_categories(empty_category_map, col_names):
    categories = col_names
    category_map = empty_category_map
    for groups in list(category_map.keys()):
        subcats = groups.split('_')
        for subcat in subcats:
            placeholder = []
            for cats in categories:
                if subcat in cats:
                    category_map[groups].append(cats)
                    placeholder.append(cats)
                    categories = list(set(categories) - set(placeholder))
    return category_map

#-----------------------------------------------------------------------------------------------------------------------
# Function to group columns based on category map

def build_grouped_df(df, category_map):
    df_grouped = df.copy()
    for group in list(category_map.keys()):
        print('\n' + group + '.....')
        if len(category_map[group]) > 0:
            try:
                df_grouped[group] = df[category_map[group][0]]
                print(category_map[group][0])
                df_grouped = df_grouped.drop(columns = [category_map[group][0]])
            except:
                df_grouped[group] = 0
                print(f'{category_map[group][0]} not found...')

            if len(category_map[group]) > 1:
                for i in range(1, len(category_map[group])):
                    try:
                        df_grouped[group] += df_grouped[category_map[group][i]]
                        print(category_map[group][i])
                        df_grouped = df_grouped.drop(columns = [category_map[group][i]])
                    except:
                        print(f'{category_map[group][i]} not found...')

    return df_grouped

#-----------------------------------------------------------------------------------------------------------------------
# Function to add Label column with 0 or 1 tag to the daraframe based on cluster score

def process_output_df(output_df, cluster_numbers_for_prediciton):

    processed_df = output_df.copy()
    num_clusters = len(pd.unique(output_df['PREDICTION']))
    columns_to_drop = ['DEPLOYMENT_APPROVAL_STATUS']
    for i in range(num_clusters):
        columns_to_drop.append(f'Cluster {i + 1}_PREDICTION')

    for cols in columns_to_drop:
        if cols in list(output_df.columns):
            processed_df.drop(columns=[cols], axis = 1, inplace = True)

    processed_df['Label'] = 0
    for i in tqdm(range(len(processed_df))):
        if processed_df['PREDICTION'][i] in cluster_numbers_for_prediciton:
            processed_df.at[i, 'Label'] = 1

    processed_df.set_index('GIGYA_ID', inplace=True, drop=True)

    return processed_df

#Function to scale specific features in a feature group
def feature_scaler(df, feature_list, feature_of_interest):

    scaled_df = df.copy()
    mean_list = scaled_df[feature_list].apply(lambda x: x.mean()).tolist()
    threshold = np.mean(mean_list)

    print(f'THERESHOLD : {threshold}, NUM_FEATURE : {len(mean_list)}')

    for features in set(feature_list) - {feature_of_interest}:

        if scaled_df[features].mean() >= threshold:
            scaled_df.loc[scaled_df[features] >=
                          scaled_df[features].mean() +
                          2 * scaled_df[features].std(), features] = 0

    return scaled_df

# Function to upscale mean of selected features
def upscale_feature_mean(df, weight_dict):
    scaled_df = df.copy()
    feature_df = scaled_df[list(weight_dict.keys())]
    mean_list = feature_df.apply(lambda x: x.mean()).tolist()
    threshold = np.mean(mean_list)

    for features in list(weight_dict.keys()):
        scaled_df[features] = scaled_df[features] * (threshold / scaled_df[features].mean())

    return scaled_df

# Function to upscale sparse features with
def scale_sparse_features(feature_group, df):
    output_df = df.copy()
    act_columns = list(set(feature_group).intersection(set(df.columns.to_list())))
    for columns in act_columns:
        output_df[columns] = ((df[columns] - df[columns].min()) / (df[columns].max() - df[columns].min())) * (
                np.max(df.max()) - np.min(df.min())) + np.min(df.min())

    return output_df


# Function to perform dimensionality reduction on original df
def reduce_df(df, n_components=(10, 15, 20, 25, 30, 40, 50)):

    for components in n_components:

        lsa = make_pipeline(TruncatedSVD(n_components=components, random_state=42), MinMaxScaler(feature_range=(0, 100)))
        reduced_df = lsa.fit_transform(df)

        explained_variance = lsa[0].explained_variance_ratio_.sum()

        if explained_variance >= 0.98:
            sv_df = pd.DataFrame(reduced_df[:, :components],
                                 columns=['PC' + str(i + 1) for i in range(components)])
            sv_df.index = df.index

            print(f'{explained_variance} variance achieved at {components} components')
            return sv_df

        else:
            print(f'{explained_variance} variance achieved at {components} components, increasing n_components')
            continue

    print('reduced df did not achieve required explained variance, returning original df')
    return df
