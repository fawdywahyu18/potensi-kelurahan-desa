# Riset dengan Dinas Penanaman Modal dan Pelayanan Terpadu Satu Pintu Kabupaten Sleman

# Converting Potensi Desa Data from dbf to csv

import csv
from dbfread import DBF
import pandas as pd

layout_data = ['desa_1', 'desa_2', 'desa_3', 'desa_4']

def convert_dbf(list_name, encoding_name):
    # list_name: list, default is layout_data
    # encoding_name: str, defaults are 'utf-8' or 'cp1252'
    
    for l_d in list_name:
        # Replace 'your_file.dbf' with the path to your DBF file
        dbf_file = f'podes2021_{l_d}.dbf'
        # Replace 'your_output.csv' with the desired CSV output file path
        csv_file = f'podes2021_{l_d}.csv'

        # Specify the non-ASCII encoding you want to use (e.g., 'utf-8')
        encoding = encoding_name # 'cp1252' is for podes2021_desa_3, 'utf-8' is for desa_1, desa_2, and desa_4 

        # Open the DBF file
        with DBF(dbf_file, encoding=encoding) as dbf:
            # Extract field names
            field_names = dbf.field_names

            # Open the CSV file for writing with the specified encoding
            with open(csv_file, 'w', newline='', encoding=encoding) as csvfile:
                writer = csv.writer(csvfile)
                
                # Write the header row with field names
                writer.writerow(field_names)

                # Iterate over records in the DBF file and write them to the CSV file
                for record in dbf:
                    # Convert record values to strings and write them to the CSV file
                    writer.writerow([str(record[field]) for field in field_names])

    return print(f"Conversion complete. The CSV file is saved at: {csv_file}")


# Data directory ada di database mikro BPS
# Open the csv file for desa Sambi Rejo (Bukan Sambirejo)
podes1_csv = pd.read_csv('podes2021_desa_1.csv', low_memory=False,
                         encoding='utf-8') # encoding method desa_1, 2, 4: 'utf-8'; encoding method desa_3:'cp1252

podes2_csv = pd.read_csv('podes2021_desa_2.csv', low_memory=False,
                         encoding='utf-8') # encoding method desa_1, 2, 4: 'utf-8'; encoding method desa_3:'cp1252

podes3_csv = pd.read_csv('podes2021_desa_3.csv', low_memory=False,
                         encoding='cp1252') # encoding method desa_1, 2, 4: 'utf-8'; encoding method desa_3:'cp1252'

podes4_csv = pd.read_csv('podes2021_desa_4.csv', low_memory=False,
                         encoding='utf-8') # encoding method desa_1, 2, 4: 'utf-8'; encoding method desa_3:'cp1252

# Subset Data
podes1_prambanan = podes1_csv[(podes1_csv['R103N']=='Prambanan') & (podes1_csv['R102N']=='Sleman')]
podes2_prambanan = podes2_csv[(podes2_csv['R103N']=='Prambanan') & (podes2_csv['R102N']=='Sleman')]
podes3_prambanan = podes3_csv[(podes3_csv['R103N']=='Prambanan') & (podes3_csv['R102N']=='Sleman')]
podes4_prambanan = podes4_csv[(podes4_csv['R103N']=='Prambanan') & (podes4_csv['R102N']=='Sleman')]

podes1_sambirejo = podes1_prambanan[podes1_prambanan['R104N']=='Sambi Rejo']
podes2_sambirejo = podes2_prambanan[podes2_prambanan['R104N']=='Sambi Rejo']
podes3_sambirejo = podes3_prambanan[podes3_prambanan['R104N']=='Sambi Rejo']
podes4_sambirejo = podes4_prambanan[podes4_prambanan['R104N']=='Sambi Rejo']

# Tabel Keterangan umum desa/kelurahan (umum)
column_names_umum = ['R104N', 'R305A', 'R305B', 'R309A', 'R309B', 'R309C', 'R403A', 'R403B']
dict_rename_umum = {'R104N':'Nama Kecamatan', 
                    'R305A':'Topografi wilayah desa kelurahan',
                    'R305B':'Keberadaan permukiman penduduk di lereng puncak',
                    'R309A':'Lokasi wilayah desa kelurahan terhadap hutan',
                    'R309B':'Fungsi Kawasan Hutan',
                    'R309C':'Ketergantungan penduduk terhadap kawawan hutan',
                    'R403A':'Sumber penghasilan utama sebagian besar penduduk desa kelurahan berasal dari lapangan usaha:',
                    'R403B':'Jenis komoditi utama sub sektor utama sebagian besar penduduk desa kelurahan'}

# Tabel Terkait perumahan dan lingkungan hidup (PLH)
column_names_plh = ['R104N', 'R502B', 'R502C', 'R504B', 'R504C', 'R504D', 'R509AK2',
                    'R509AK3', 'R509AK4', 'R509AK5', 'R510C1', 'R512A',
                    'R513AK2', 'R513BK2', 'R513CK2']
dict_rename_plh = {'R104N':'Nama Kecamatan', 
                   'R502B':'Penerangan di jalan utama desa kelurahan',
                   'R502C':'Sumber penerangan di jalan utama desa kelurahan',
                   'R504B':'Tempat buang sampah sebagian besar keluarga',
                   'R504C':'Tempat penampungan sampah sementara (TPS) :',
                   'R504D':'Keberadaan bank sampah di desa kelurahan',
                   'R509AK2':'Keberadaan sungai :',
                   'R509AK3':'Keberadaan saluran irigasi :',
                   'R509AK4':'Keberadaan danau/waduk/situ/bendungan :', 
                   'R509AK5':'Keberadaan embung :', 
                   'R510C1':'Air sungai tercemar limbah:', 
                   'R512A':'Keberadaan permukiman kumuh :',
                   'R513AK2':'Kejadian pencemaran lingkungan : Air', 
                   'R513BK2':'Kejadian pencemaran lingkungan : Tanah',
                   'R513CK2':'Kejadian pencemaran lingkungan : Udara'}

# Tabel terkait Bencana Alam (BA)
column_names_ba = ['R104N', 'R601AK2', 'R601BK2', 'R601CK2', 'R601DK2', 'R504D']
dict_rename_ba = {'R104N':'Nama Kecamatan', 
                  'R601AK2':'Tanah longsor', 
                  'R601BK2':'Banjir', 
                  'R601CK2':'Banjir Bandang', 
                  'R601DK2':'Gempa Bumi', 
                  'R504D':'Rambu-rambu dan jalur evakuasi bencana'}


# Tabel terkait Sosial Budaya (SB)
column_names_sb = ['R104N', 'R809A', 'R809B', 'R809C', 'R809D', 'R809E', 'R809F']
dict_rename_sb = {'R104N':'Nama Kecamatan', 
                  'R809A':'Jumlah jenis-jenis lembaga kemasyarakatan desa: PKK', 
                  'R809B':'Jumlah jenis-jenis lembaga kemasyarakatan desa: Karang Taruna', 
                  'R809C':'Jumlah jenis-jenis lembaga kemasyarakatan desa: Lembaga Adat', 
                  'R809D':'Jumlah jenis-jenis lembaga kemasyarakatan desa: Kelompok Tani', 
                  'R809E':'Jumlah jenis-jenis lembaga kemasyarakatan desa: Lembaga Pengelolaan Air', 
                  'R809F':'Jumlah jenis-jenis lembaga kemasyarakatan desa: Kelompok Masyarakat (Pokmas)'}

# Tabel terkait Angkutan, Komunikasi, dan Informasi (AKI)
column_names_aki = ['R104N', 'R1001A', 'R1001B1', 'R1001B2', 'R1001C1', 'R1001C2',
                    'R1003B', 'R1004', 'R1005A', 'R1005B', 'R1005C', 'R1005D']
dict_rename_aki = {'R104N':'Nama Kecamatan', 
                   'R1001A':'Lalu lintas dari  ke desa  kelurahan melalui :', 
                   'R1001B1':'Jenis permukaan jalan yang terluas :', 
                   'R1001B2':'Jalan darat dapat dilalui kendaraan bermotor roda 4 atau lebih :', 
                   'R1001C1':'Keberadaan angkutan umum :', 
                   'R1001C2':'Operasional angkutan umum yang utama :',
                   'R1003B':'Keberadaan warga yang menggunakan telepon seluler/handphone', 
                   'R1004':'Keberadaan internet untuk warnet, game online, dan fasilitas lainnya di desa  kelurahan', 
                   'R1005A':'Jumlah menara Base Transceiver Station (BTS)', 
                   'R1005B':'Jumlah operator layanan komunikasi telepon seluler handphone yang menjangkau', 
                   'R1005C':'Sinyal telepon seluler handphone di sebagian besar wilayah desa kelurahan', 
                   'R1005D':'Sinyal internet telepon seluler handphone di sebagian besar wilayah di desa kelurahan:'}

# Tabel terkait Kondisi Ekonomi (KE)
column_names_ke = ['R104N', 'R1202A', 'R1202B', 'R1202C', 'R1203A', 'R1203B1', 'R1203B2',
                   'R1207AK2', 'R1207EK2', 'R1207GK2', 'R1207JK2']
dict_rename_ke = {'R104N':'Nama Kecamatan', 
                  'R1202A':'Jumlah Sentra Industri:', 
                  'R1202B':'Jumlah Lingkungan Industri Kecil (LIK) :', 
                  'R1202C':'Jumlah Perkampungan Industri Kecil (PIK) :', 
                  'R1203A':'Keberadaan produk barang unggulan utama di desa kelurahan', 
                  'R1203B1':'Produk unggalan makanan ....', 
                  'R1203B2':'Produk unggalan non makanan ....',
                  'R1207AK2':'Jumlah kelompok pertokoan', 
                  'R1207EK2':'Jumlah minimarket swalayan supermarket', 
                  'R1207GK2':'Jumlah Warung kedai makanan minuman', 
                  'R1207JK2':'Jumlah Toko warung kelontong'}

# Tabel terkait identifikasi industri di kelurahan (Industri)
column_names_industri = ['R104N', 'R1201A', 'R1201B', 'R1201C', 'R1201D', 'R1201E',
                         'R1201F', 'R1201G', 'R1201H', 'R1201I', 'R1201J',
                         'R1201K', 'R1201L', 'R1201M', 'R1201N', 'R1201O']
dict_rename_industri = {'R104N':'Nama Kecamatan',
                        'R1201A':'Industri mikro dan kecil kulit, dan barang dari kulit (tas, sepatu, sandal, dll.)', 
                        'R1201B':'Industri mikro dan kecil furnitur dari kayu, rotan  bambu, plastik, logam (meja, kursi, tempat tidur, lemari, dll)', 
                        'R1201C':'Industri mikro dan kecil barang logam, bukan mesin dan peralatannya (teralis, pagar, sabit, pisau, dll', 
                        'R1201D':'Industri mikro dan kecil tekstil (kain ulos, kain songket, kain tenun, dan percetakan batik, dll)', 
                        'R1201E':'Industri mikro dan kecil pakaian jadi (konveksi, pakaian, kemeja, rok, celana, mukena bordir)',
                        'R1201F':'Industri mikro dan kecil barang galian bukan logam  industri gerabah  keramik  batu bata (genteng, batu bata, porselin, dll', 
                        'R1201G':'Industri mikro dan kecil kayu, barang dari kayu, barang anyaman dari bambu, rotan dan sejenisnya (reng kayu, papan, dll', 
                        'R1201H':'Industri mikro dan kecil makanan (pengolahan dan pengawetan daging, ikan, buah,sayuran, minyak dan lemak, susu, dll', 
                        'R1201I':'Industri mikro dan kecil minuman (minuman kemasan, air mineral, air isi ulang, sopi dll)', 
                        'R1201J':'Industri mikro dan kecil pengolahan tembakau (industri rokok, pengeringan dan perajangan tembakau)',
                        'R1201K':'Industri mikro dan kecil kertas dan barang dari kertas (kantong kertas, post card, kardus, sak semen)', 
                        'R1201L':'Industri mikro dan kecil percetakan dan reproduksi media rekaman (buku, brosur, kartu nama, kalender, spanduk, dll)', 
                        'R1201M':'Industri mikro dan kecil alat angkutan lainnya (perahu, klotok, rakit, kursi roda, dll)', 
                        'R1201N':'Industri mikro dan kecil kerajinan dan lainnya (kerajinan tangan, mainan anak-anak, batu akik, perhiasan emas  imitasi,)', 
                        'R1201O':'Reparasi dan pemasangan mesin dan peralatan (las keliling, reparasi dinamo, reparasi mesin penggiling padi, dll'}

# Tabel terkait Keamanan
column_names_keamanan = ['R104N', 'R1303A01K3', 'R1303A02K3', 'R1303A03K3',
                         'R1303A04K3', 'R1303A05K3', 'R1303A06K3', 'R1303A08K3',
                         'R1303A09K3',
                         'R1303B', 'R1304A', 'R1304B', 'R1304C', 'R1304E',
                         'R1305', 'R1306A', 'R1306B1', 'R1306C1']
dict_rename_keamanan = {'R104N':'Nama Kecamatan', 
                        'R1303A01K3':'Kejadian tindak pencurian yang terjadi di desa kelurahan selama setahun terakhir:', 
                        'R1303A02K3':'Kejadian tindak pencurian dengan kekerasan yang terjadi di desa kelurahan selama setahun terakhir:', 
                        'R1303A03K3':'Kejadian tindak penipuan penggelapan yang terjadi di desa kelurahan selama setahun terakhir:',
                        'R1303A04K3':'Kejadian tindak penganiayaan yang terjadi di desa kelurahan selama setahun terakhir:', 
                        'R1303A05K3':'Kejadian tindak pembakaran yang terjadi di desa kelurahan selama setahun terakhir:', 
                        'R1303A06K3':'Kejadian tindak perkosaan kejahatan terhadap kesusilaan yang terjadi di desa kelurahan selama setahun terakhir:', 
                        'R1303A08K3':'Kejadian tindak perjudian yang terjadi di desa kelurahan selama setahun terakhir:',
                        'R1303A09K3':'Kejadian tindak pembunuhan yang terjadi di desa kelurahan selama setahun terakhir:',
                        'R1303B':'Tindak kejahatan yang paling sering terjadi:', 
                        'R1304A':'Pembangunan  pemeliharaan pos keamanan lingkungan:', 
                        'R1304B':'Pembentukan  pengaturan regu keamanan:', 
                        'R1304C':'Penambahan jumlah anggota hansip  linmas:', 
                        'R1304E':'Pengaktifan sistem keamanan lingkungan berasal dari inisiatif warga:',
                        'R1305':'Jumlah anggota linmas hansip di desa kelurahan :  ....  orang', 
                        'R1306A':'Keberadaan pos polisi (termasuk kantor polisi)', 
                        'R1306B1':'Jumlah pos polisi (termasuk kantor polisi) yang digunakan', 
                        'R1306C1':'Perkiraan jarak ke pos polisi (termasuk kantor polisi) terdekat  : ....  Km'}

# Function untuk membuat tabel
def tabel_prambanan(podes_df, list_column_name, dict_rename_column,
                    export_directory):
    # podes_df: dataframe, contoh podes1_prambanan
    # list_clumn_name: list, contoh column_name_umum
    # dict_rename_column: dict, contoh dict_rename_umum
    # export directory: str, contoh 'Volumes/Backup Plus/Backup Fawdy/Kerja/Bulaksumur Consulting/Dinas Penanaman Modal Sleman/Tabel/tabel keamanan.xlsx'
    
    # podes_df = podes1_prambanan
    # list_column_name = column_names_umum
    
    podes_selected = podes_df[list_column_name]
    list_rename = list(dict_rename_column.values())
    podes_selected.columns = list_rename
    
    podes_melt = pd.melt(podes_selected, id_vars=['Nama Kecamatan'], value_vars=list_rename[1:])
    podes_melt.columns = ['Nama Kelurahan/Desa', 'Variabel', 'Nilai']
    
    podes_melt.to_excel(export_directory, index=False)
    return podes_melt

# Running function
# Main export directory ada di filder bulaksumur consulting
tabel_umum = tabel_prambanan(podes1_sambirejo, column_names_umum, dict_rename_umum,
                             'Tabel/Tabel Umum.xlsx')

tabel_plh = tabel_prambanan(podes1_sambirejo, column_names_plh, dict_rename_plh,
                            'Tabel/Tabel PLH.xlsx')

tabel_ba = tabel_prambanan(podes1_sambirejo, column_names_ba, dict_rename_ba,
                           'Tabel/Tabel Bencana Alam.xlsx')

tabel_sb = tabel_prambanan(podes3_sambirejo, column_names_sb, dict_rename_sb,
                           'Tabel/Tabel Sosial Budaya.xlsx')

tabel_aki = tabel_prambanan(podes3_sambirejo, column_names_aki, dict_rename_aki,
                           'Tabel/Tabel Akses Komunikasi Informasi.xlsx')

tabel_ke = tabel_prambanan(podes3_sambirejo, column_names_ke, dict_rename_ke,
                           'Tabel/Tabel Kondisi Ekonomi.xlsx')

tabel_industri = tabel_prambanan(podes3_sambirejo, column_names_industri, dict_rename_industri,
                                 'Tabel/Tabel Industri Sambirejo.xlsx')

tabel_keamanan = tabel_prambanan(podes4_sambirejo, column_names_keamanan, dict_rename_keamanan,
                                 'Tabel/Tabel Kondisi Keamanan.xlsx')

