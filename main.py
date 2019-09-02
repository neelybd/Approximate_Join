import pandas as pd
from tkinter import Tk
from tkinter.filedialog import asksaveasfilename, askopenfilename

def main():
    print("Program: Approximate Join")
    print("Release: 0.0.2")
    print("Date: 2019-09-02")
    print("Author: Brian Neely")
    print()
    print()
    print("This program reads two csv file and joins them based on a key and a nearest match.")
    print()
    print()

    # Hide Tkinter GUI
    Tk().withdraw()

    # Find input file 1
    file_in_1 = select_file_in("Select file 1")

    # Find input file 2
    file_in_2 = select_file_in("Select file 2")

    # Set output file
    file_out = select_file_out(file_in_1)

    # Ask for delimination
    delimination = input("Enter Deliminator: ")

    # Open input csv using the unknown encoder function
    print("Opening " + file_in_1)
    data_1 = open_unknown_csv(file_in_1, delimination)
    print(file_in_1 + " Opened")
    print("")
    print("Opening " + file_in_2)
    data_2 = open_unknown_csv(file_in_2, delimination)
    print(file_in_2 + " Opened")

    # Select Table 1's join key 1
    table_1_join_key_1 = column_selection(data_1, "Select Table 1 Join Key 1 - Used for fuzzy date match")

    # Select Table 2's join key 1
    table_2_join_key_1 = column_selection(data_2, "Select Table 2 Join Key 1 - Used for fuzzy date match")

    # Select Primary Table
    if y_n_question("Choose Table 1 [" + file_in_1 + "] as Primary Table (Yes/No): ") == True:
        primary = data_1
        primary_key = table_1_join_key_1
        secondary = data_2
        secondary_key = table_2_join_key_1
    else:
        primary = data_2
        primary_key = table_2_join_key_1
        secondary = data_1
        secondary_key = table_1_join_key_1

    # Ask for secondary join keys
    if y_n_question("Second Join Key (Yes/No): ") == True:
        # Select Table 1's join key 1
        primary_key_2 = column_selection(data_1, "Select Primary Table Second Join Key")

        # Select Table 2's join key 1
        secondary_key_2 = column_selection(data_2, "Select Secondary Table Second Join Key")

        # Flag for second join key
        second_join_key = True

    else:
        # Flag for second join key
        second_join_key = False

    # Include inner results
    inner = y_n_question("Include inner results (Yes/No): ")

    # Convert table keys to numeric
    if y_n_question("Is Table Key 1 a Datetime (Yes/No): ") == True:
        primary[primary_key] = convert_to_datetime(primary[primary_key])
        secondary[secondary_key] = convert_to_datetime(secondary[secondary_key])
    else:
        print("Non-numeric values will be set to NaN and exported as a separate file.")
        primary[primary_key] = pd.to_numeric(primary[primary_key], errors='coerce')
        secondary[secondary_key] = pd.to_numeric(secondary[secondary_key], errors='coerce')

    # Print the head of the keys
    print("")
    print("Printing Head of Primary Table Key")
    print(primary[primary_key].head())
    print("")
    print("Printing Head of Secondary Table Key")
    print(secondary[secondary_key].head())
    print("")

    # *****Need to roll this into a function and put a manual check with it to ensure that the data is the right format.

    # Rename Secondary Key to Primary Key for join
    secondary.rename(columns={secondary_key:primary_key}, inplace=True)

    # Select round type, Nearest, Round-down, Round-up, Nearest
    fuzzy_join_type = indexed_question("Select Join Type", ["Nearest", "Nearest where Primary > Secondary",
                                                            "Nearest where Primary < Secondary"])
    if fuzzy_join_type == "Nearest":
        direction = "nearest"
    if fuzzy_join_type == "Nearest where Primary > Secondary":
        direction = "forward"
    if fuzzy_join_type == "Nearest where Primary < Secondary":
        direction = "backward"

    # Separate data files based on Second Join Key
    if second_join_key == True:
        # Split dataframes by the second join keys.
        # 1. Pull the secondary joins keys
        # 2. Union keys from both tables
        # 3. Dedup list
        # 3. Sort Keys
        # 4. Make a list of dataframes split based on the join keys
        # 5. Perform the join on a per split dataframe.
        # 6. Union split dataframes

        # Pull the secondary join keys and dedup
        primary_key_2_list = primary[primary_key_2].drop_duplicates()
        secondary_key_2_list = secondary[secondary_key_2].drop_duplicates()

        # Union keys from both tables and dedup
        key_2_list = primary_key_2_list.append(secondary_key_2_list).drop_duplicates()

        # Sort Keys
        key_2_list.sort_values(inplace=True)

        # Split dataframes based on second key
        # Create empty arrays for new dataframes
        primary_df_array = {}
        secondary_df_array = {}
        for index, i in enumerate(key_2_list):
            print(str(i) + " " + str(index))
            primary_df_array[index] = primary[primary[primary_key_2] == i]
            secondary_df_array[index] = secondary[secondary[secondary_key_2] == i]

        # Merge Datasets
        # Create empty merged dataframe array
        merged_df_array = {}
        for index, i in enumerate(primary_df_array):
            merged_df_array[index] = pd.merge_asof(primary_df_array[index], secondary_df_array[index], on=primary_key,
                                               direction=direction, allow_exact_matches=inner)

        # Union split dataframes
        data_out = pd.concat(merged_df_array, ignore_index=True, sort=False).reset_index(drop=True)

    else:
        # Merge Datasets
        data_out = pd.merge_asof(primary, secondary, on=primary_key, direction=direction, allow_exact_matches=inner)

    # Export File
    data_out.to_csv(file_out, index=False)


def convert_to_datetime(data):
    # Try to autodetect datetime format
    try:
        data_converted = pd.to_datetime(data, infer_datetime_format=True)
    except:
        print("Date time format could not be automatically be determined.")
        while True:
            try:
                date_format = input("Input Date Format. Example = %Y%m%d")
                date_converted = pd.to_datetime(data, format=date_format)
            except:
                print("Input Date Format invalid. Please try again.")
                continue
            else:
                break
    return data_converted



def column_selection(data, title):
    # Create Column Header List
    headers = list(data.columns.values)
    while True:
        try:
            print(title)
            for j, i in enumerate(headers):
                print(str(j) + ": to select column [" + str(i) + "]")
            column = headers[int(input("Enter Selection: "))]
        except ValueError:
                print("Input must be integer between 0 and " + str(len(headers)))
                continue
        else:
            break
    return column


def select_file_in(title):
    file_in = askopenfilename(initialdir="../", title=title,
                              filetypes=(("Comma Separated Values", "*.csv"), ("all files", "*.*")))
    if not file_in:
        input("Program Terminated. Press Enter to continue...")
        exit()

    return file_in


def select_file_out(file_in):
    file_out = asksaveasfilename(initialdir=file_in, title="Select file",
                                 filetypes=(("Comma Separated Values", "*.csv"), ("all files", "*.*")))
    if not file_out:
        input("Program Terminated. Press Enter to continue...")
        exit()

    # Create an empty output file
    open(file_out, 'a').close()

    return file_out


def open_unknown_csv(file_in, delimination):
    encode_index = 0
    encoders = ['utf_8', 'latin1', 'utf_16',
                'ascii', 'big5', 'big5hkscs', 'cp037', 'cp424',
                'cp437', 'cp500', 'cp720', 'cp737', 'cp775',
                'cp850', 'cp852', 'cp855', 'cp856', 'cp857',
                'cp858', 'cp860', 'cp861', 'cp862', 'cp863',
                'cp864', 'cp865', 'cp866', 'cp869', 'cp874',
                'cp875', 'cp932', 'cp949', 'cp950', 'cp1006',
                'cp1026', 'cp1140', 'cp1250', 'cp1251', 'cp1252',
                'cp1253', 'cp1254', 'cp1255', 'cp1256', 'cp1257',
                'cp1258', 'euc_jp', 'euc_jis_2004', 'euc_jisx0213', 'euc_kr',
                'gb2312', 'gbk', 'gb18030', 'hz', 'iso2022_jp',
                'iso2022_jp_1', 'iso2022_jp_2', 'iso2022_jp_2004', 'iso2022_jp_3', 'iso2022_jp_ext',
                'iso2022_kr', 'latin_1', 'iso8859_2', 'iso8859_3', 'iso8859_4',
                'iso8859_5', 'iso8859_6', 'iso8859_7', 'iso8859_8', 'iso8859_9',
                'iso8859_10', 'iso8859_11', 'iso8859_13', 'iso8859_14', 'iso8859_15',
                'iso8859_16', 'johab', 'koi8_r', 'koi8_u', 'mac_cyrillic',
                'mac_greek', 'mac_iceland', 'mac_latin2', 'mac_roman', 'mac_turkish',
                'ptcp154', 'shift_jis', 'shift_jis_2004', 'shift_jisx0213', 'utf_32',
                'utf_32_be', 'utf_32_le', 'utf_16', 'utf_16_be', 'utf_16_le',
                'utf_7', 'utf_8', 'utf_8_sig']

    data = open_file(file_in, encoders[encode_index], delimination)
    while data is str:
        if encode_index < len(encoders) - 1:
            encode_index = encode_index + 1
            data = open_file(file_in, encoders[encode_index], delimination)
        else:
            print("Can't find appropriate encoder")
            exit()

    return data


def open_file(file_in, encoder, delimination):
    try:
        data = pd.read_csv(file_in, low_memory=False, encoding=encoder, delimiter=delimination)
        print("Opened file using encoder: " + encoder)

    except UnicodeDecodeError:
        print("Encoder Error for: " + encoder)
        return "Encode Error"

    return data


def y_n_question(question):
    while True:
        # Ask question
        answer = input(question)
        answer_cleaned = answer[0].lower()
        if answer_cleaned == 'y' or answer_cleaned == 'n':
            if answer_cleaned == 'y':
                return True
            if answer_cleaned == 'n':
                return False
        else:
            print("Invalid input, please try again.")


def indexed_question(question, answer_list):
    while True:
        try:
            print(question)
            for j, i in enumerate(answer_list):
                print(str(j) + ": to select [" + str(i) + "]")
            column = answer_list[int(input("Enter Selection: "))]
        except IndexError:
                print("Input must be integer between 0 and " + str(len(answer_list)-1))
                continue
        else:
            break
    return column


if __name__ == '__main__':
    main()
