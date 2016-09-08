package org.apache.kudu.mapreduce.tools;

import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;
import org.apache.kudu.mapreduce.CommandLineParser;
import org.apache.kudu.mapreduce.KuduTableMapReduceUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Map-only job that reads CSV files and inserts them into a single Kudu table.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ImportCsv extends Configured implements Tool {

  public static enum Counters { BAD_LINES };

  static final String NAME = "importcsv";
  static final String DEFAULT_SEPARATOR = ",";
  static final String SEPARATOR_CONF_KEY = "importcsv.separator";
  static final String JOB_NAME_CONF_KEY = "importcsv.job.name";
  static final String SKIP_LINES_CONF_KEY = "importcsv.skip.bad.lines";
  static final String COLUMNS_NAMES_KEY = "importcsv.column.names";

  /**
   * Sets up the actual job.
   *
   * @param conf The current configuration.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws java.io.IOException When setting up the job fails.
   */
  @SuppressWarnings("deprecation")
  public static Job createSubmittableJob(Configuration conf, String[] args)
      throws IOException, ClassNotFoundException {

    Class<ImportCsvMapper> mapperClass = ImportCsvMapper.class;
    String columns1 = "fv_detail_id,fv_equid,fv_eqtype,fv_eqinfo,fv_date,fv_time,fv_dayofweek,fv_timestamp,fv_itimestamp,fv_longitude,fv_latitude,fv_height,fv_freq_start,fv_freq_end,fv_freq_step,fv_freq_count,fv_collect_type,fv_value_type,fv_debug,fv_30,fv_31,fv_32,fv_33,fv_34,fv_35,fv_36,fv_37,fv_38,fv_39,fv_40,fv_41,fv_42,fv_43,fv_44,fv_45,fv_46,fv_47,fv_48,fv_49,fv_50,fv_51,fv_52,fv_53,fv_54,fv_55,fv_56,fv_57,fv_58,fv_59,fv_60,fv_61,fv_62,fv_63,fv_64,fv_65,fv_66,fv_67,fv_68,fv_69,fv_70,fv_71,fv_72,fv_73,fv_74,fv_75,fv_76,fv_77,fv_78,fv_79,fv_80,fv_81,fv_82,fv_83,fv_84,fv_85,fv_86,fv_87,fv_88,fv_89,fv_90,fv_91,fv_92,fv_93,fv_94,fv_95,fv_96,fv_97,fv_98,fv_99,fv_100,fv_101,fv_102,fv_103,fv_104,fv_105,fv_106,fv_107,fv_108,fv_109,fv_110,fv_111,fv_112,fv_113,fv_114,fv_115,fv_116,fv_117,fv_118,fv_119,fv_120,fv_121,fv_122,fv_123,fv_124,fv_125,fv_126,fv_127,fv_128,fv_129,fv_130,fv_131,fv_132,fv_133,fv_134,fv_135,fv_136,fv_137,fv_138,fv_139,fv_140,fv_141,fv_142,fv_143,fv_144,fv_145,fv_146,fv_147,fv_148,fv_149,fv_150,fv_151,fv_152,fv_153,fv_154,fv_155,fv_156,fv_157,fv_158,fv_159,fv_160,fv_161,fv_162,fv_163,fv_164,fv_165,fv_166,fv_167,fv_168,fv_169,fv_170,fv_171,fv_172,fv_173,fv_174,fv_175,fv_176,fv_177,fv_178,fv_179,fv_180,fv_181,fv_182,fv_183,fv_184,fv_185,fv_186,fv_187,fv_188,fv_189,fv_190,fv_191,fv_192,fv_193,fv_194,fv_195,fv_196,fv_197,fv_198,fv_199,fv_200,fv_201,fv_202,fv_203,fv_204,fv_205,fv_206,fv_207,fv_208,fv_209,fv_210,fv_211,fv_212,fv_213,fv_214,fv_215,fv_216,fv_217,fv_218,fv_219,fv_220,fv_221,fv_222,fv_223,fv_224,fv_225,fv_226,fv_227,fv_228,fv_229,fv_230,fv_231,fv_232,fv_233,fv_234,fv_235,fv_236,fv_237,fv_238,fv_239,fv_240,fv_241,fv_242,fv_243,fv_244,fv_245,fv_246,fv_247,fv_248,fv_249,fv_250,fv_251,fv_252,fv_253,fv_254,fv_255,fv_256,fv_257,fv_258,fv_259,fv_260,fv_261,fv_262,fv_263,fv_264,fv_265,fv_266,fv_267,fv_268,fv_269,fv_270,fv_271,fv_272,fv_273,fv_274,fv_275,fv_276,fv_277,fv_278,fv_279,fv_280,fv_281,fv_282,fv_283,fv_284,fv_285,fv_286,fv_287,fv_288,fv_289,fv_290,fv_291,fv_292,fv_293,fv_294,fv_295,fv_296,fv_297,fv_298,fv_299,fv_300,fv_301,fv_302,fv_303,fv_304,fv_305,fv_306,fv_307,fv_308,fv_309,fv_310,fv_311,fv_312,fv_313,fv_314,fv_315,fv_316,fv_317,fv_318,fv_319,fv_320,fv_321,fv_322,fv_323,fv_324,fv_325,fv_326,fv_327,fv_328,fv_329,fv_330,fv_331,fv_332,fv_333,fv_334,fv_335,fv_336,fv_337,fv_338,fv_339,fv_340,fv_341,fv_342,fv_343,fv_344,fv_345,fv_346,fv_347,fv_348,fv_349,fv_350,fv_351,fv_352,fv_353,fv_354,fv_355,fv_356,fv_357,fv_358,fv_359,fv_360,fv_361,fv_362,fv_363,fv_364,fv_365,fv_366,fv_367,fv_368,fv_369,fv_370,fv_371,fv_372,fv_373,fv_374,fv_375,fv_376,fv_377,fv_378,fv_379,fv_380,fv_381,fv_382,fv_383,fv_384,fv_385,fv_386,fv_387,fv_388,fv_389,fv_390,fv_391,fv_392,fv_393,fv_394,fv_395,fv_396,fv_397,fv_398,fv_399,fv_400,fv_401,fv_402,fv_403,fv_404,fv_405,fv_406,fv_407,fv_408,fv_409,fv_410,fv_411,fv_412,fv_413,fv_414,fv_415,fv_416,fv_417,fv_418,fv_419,fv_420,fv_421,fv_422,fv_423,fv_424,fv_425,fv_426,fv_427,fv_428,fv_429,fv_430,fv_431,fv_432,fv_433,fv_434,fv_435,fv_436,fv_437,fv_438,fv_439,fv_440,fv_441,fv_442,fv_443,fv_444,fv_445,fv_446,fv_447,fv_448,fv_449,fv_450,fv_451,fv_452,fv_453,fv_454,fv_455,fv_456,fv_457,fv_458,fv_459,fv_460,fv_461,fv_462,fv_463,fv_464,fv_465,fv_466,fv_467,fv_468,fv_469,fv_470,fv_471,fv_472,fv_473,fv_474,fv_475,fv_476,fv_477,fv_478,fv_479,fv_480,fv_481,fv_482,fv_483,fv_484,fv_485,fv_486,fv_487,fv_488,fv_489,fv_490,fv_491,fv_492,fv_493,fv_494,fv_495,fv_496,fv_497,fv_498,fv_499,fv_500,fv_501,fv_502,fv_503,fv_504,fv_505,fv_506,fv_507,fv_508,fv_509,fv_510,fv_511,fv_512,fv_513,fv_514,fv_515,fv_516,fv_517,fv_518,fv_519,fv_520,fv_521,fv_522,fv_523,fv_524,fv_525,fv_526,fv_527,fv_528,fv_529,fv_530,fv_531,fv_532,fv_533,fv_534,fv_535,fv_536,fv_537,fv_538,fv_539,fv_540,fv_541,fv_542,fv_543,fv_544,fv_545,fv_546,fv_547,fv_548,fv_549,fv_550,fv_551,fv_552,fv_553,fv_554,fv_555,fv_556,fv_557,fv_558,fv_559,fv_560,fv_561,fv_562,fv_563,fv_564,fv_565,fv_566,fv_567,fv_568,fv_569,fv_570,fv_571,fv_572,fv_573,fv_574,fv_575,fv_576,fv_577,fv_578,fv_579,fv_580,fv_581,fv_582,fv_583,fv_584,fv_585,fv_586,fv_587,fv_588,fv_589,fv_590,fv_591,fv_592,fv_593,fv_594,fv_595,fv_596,fv_597,fv_598,fv_599,fv_600,fv_601,fv_602,fv_603,fv_604,fv_605,fv_606,fv_607,fv_608,fv_609,fv_610,fv_611,fv_612,fv_613,fv_614,fv_615,fv_616,fv_617,fv_618,fv_619,fv_620,fv_621,fv_622,fv_623,fv_624,fv_625,fv_626,fv_627,fv_628,fv_629,fv_630,fv_631,fv_632,fv_633,fv_634,fv_635,fv_636,fv_637,fv_638,fv_639,fv_640,fv_641,fv_642,fv_643,fv_644,fv_645,fv_646,fv_647,fv_648,fv_649,fv_650,fv_651,fv_652,fv_653,fv_654,fv_655,fv_656,fv_657,fv_658,fv_659,fv_660,fv_661,fv_662,fv_663,fv_664,fv_665,fv_666,fv_667,fv_668,fv_669,fv_670,fv_671,fv_672,fv_673,fv_674,fv_675,fv_676,fv_677,fv_678,fv_679,fv_680,fv_681,fv_682,fv_683,fv_684,fv_685,fv_686,fv_687,fv_688,fv_689,fv_690,fv_691,fv_692,fv_693,fv_694,fv_695,fv_696,fv_697,fv_698,fv_699,fv_700,fv_701,fv_702,fv_703,fv_704,fv_705,fv_706,fv_707,fv_708,fv_709,fv_710,fv_711,fv_712,fv_713,fv_714,fv_715,fv_716,fv_717,fv_718,fv_719,fv_720,fv_721,fv_722,fv_723,fv_724,fv_725,fv_726,fv_727,fv_728,fv_729,fv_730,fv_731,fv_732,fv_733,fv_734,fv_735,fv_736,fv_737,fv_738,fv_739,fv_740,fv_741,fv_742,fv_743,fv_744,fv_745,fv_746,fv_747,fv_748,fv_749,fv_750,fv_751,fv_752,fv_753,fv_754,fv_755,fv_756,fv_757,fv_758,fv_759,fv_760,fv_761,fv_762,fv_763,fv_764,fv_765,fv_766,fv_767,fv_768,fv_769,fv_770,fv_771,fv_772,fv_773,fv_774,fv_775,fv_776,fv_777,fv_778,fv_779,fv_780,fv_781,fv_782,fv_783,fv_784,fv_785,fv_786,fv_787,fv_788,fv_789,fv_790,fv_791,fv_792,fv_793,fv_794,fv_795,fv_796,fv_797,fv_798,fv_799,fv_800,fv_801,fv_802,fv_803,fv_804,fv_805,fv_806,fv_807,fv_808,fv_809,fv_810,fv_811,fv_812,fv_813,fv_814,fv_815,fv_816,fv_817,fv_818,fv_819,fv_820,fv_821,fv_822,fv_823,fv_824,fv_825,fv_826,fv_827,fv_828,fv_829,fv_830,fv_831,fv_832,fv_833,fv_834,fv_835,fv_836,fv_837,fv_838,fv_839,fv_840,fv_841,fv_842,fv_843,fv_844,fv_845,fv_846,fv_847,fv_848,fv_849,fv_850,fv_851,fv_852,fv_853,fv_854,fv_855,fv_856,fv_857,fv_858,fv_859,fv_860,fv_861,fv_862,fv_863,fv_864,fv_865,fv_866,fv_867,fv_868,fv_869,fv_870,fv_871,fv_872,fv_873,fv_874,fv_875,fv_876,fv_877,fv_878,fv_879,fv_880,fv_881,fv_882,fv_883,fv_884,fv_885,fv_886,fv_887,fv_888,fv_889,fv_890,fv_891,fv_892,fv_893,fv_894,fv_895,fv_896,fv_897,fv_898,fv_899,fv_900,fv_901,fv_902,fv_903,fv_904,fv_905,fv_906,fv_907,fv_908,fv_909,fv_910,fv_911,fv_912,fv_913,fv_914,fv_915,fv_916,fv_917,fv_918,fv_919,fv_920,fv_921,fv_922,fv_923,fv_924,fv_925,fv_926,fv_927,fv_928,fv_929,fv_930,fv_931,fv_932,fv_933,fv_934,fv_935,fv_936,fv_937,fv_938,fv_939,fv_940,fv_941,fv_942,fv_943,fv_944,fv_945,fv_946,fv_947,fv_948,fv_949,fv_950,fv_951,fv_952,fv_953,fv_954,fv_955,fv_956,fv_957,fv_958,fv_959,fv_960,fv_961,fv_962,fv_963,fv_964,fv_965,fv_966,fv_967,fv_968,fv_969,fv_970,fv_971,fv_972,fv_973,fv_974,fv_975,fv_976,fv_977,fv_978,fv_979,fv_980,fv_981,fv_982,fv_983,fv_984,fv_985,fv_986,fv_987,fv_988,fv_989,fv_990,fv_991,fv_992,fv_993,fv_994,fv_995,fv_996,fv_997,fv_998,fv_999,fv_1000,fv_1001,fv_1002,fv_1003,fv_1004,fv_1005,fv_1006,fv_1007,fv_1008,fv_1009,fv_1010,fv_1011,fv_1012,fv_1013,fv_1014,fv_1015,fv_1016,fv_1017,fv_1018,fv_1019,fv_1020,fv_1021,fv_1022,fv_1023,fv_1024,fv_1025,fv_1026,fv_1027,fv_1028,fv_1029,fv_1030,fv_1031,fv_1032,fv_1033,fv_1034,fv_1035,fv_1036,fv_1037,fv_1038,fv_1039,fv_1040,fv_1041,fv_1042,fv_1043,fv_1044,fv_1045,fv_1046,fv_1047,fv_1048,fv_1049,fv_1050,fv_1051,fv_1052,fv_1053,fv_1054,fv_1055,fv_1056,fv_1057,fv_1058,fv_1059,fv_1060,fv_1061,fv_1062,fv_1063,fv_1064,fv_1065,fv_1066,fv_1067,fv_1068,fv_1069,fv_1070,fv_1071,fv_1072,fv_1073,fv_1074,fv_1075,fv_1076,fv_1077,fv_1078,fv_1079,fv_1080,fv_1081,fv_1082,fv_1083,fv_1084,fv_1085,fv_1086,fv_1087,fv_1088,fv_1089,fv_1090,fv_1091,fv_1092,fv_1093,fv_1094,fv_1095,fv_1096,fv_1097,fv_1098,fv_1099,fv_1100,fv_1101,fv_1102,fv_1103,fv_1104,fv_1105,fv_1106,fv_1107,fv_1108,fv_1109,fv_1110,fv_1111,fv_1112,fv_1113,fv_1114,fv_1115,fv_1116,fv_1117,fv_1118,fv_1119,fv_1120,fv_1121,fv_1122,fv_1123,fv_1124,fv_1125,fv_1126,fv_1127,fv_1128,fv_1129,fv_1130,fv_1131,fv_1132,fv_1133,fv_1134,fv_1135,fv_1136,fv_1137,fv_1138,fv_1139,fv_1140,fv_1141,fv_1142,fv_1143,fv_1144,fv_1145,fv_1146,fv_1147,fv_1148,fv_1149,fv_1150,fv_1151,fv_1152,fv_1153,fv_1154,fv_1155,fv_1156,fv_1157,fv_1158,fv_1159,fv_1160,fv_1161,fv_1162,fv_1163,fv_1164,fv_1165,fv_1166,fv_1167,fv_1168,fv_1169,fv_1170,fv_1171,fv_1172,fv_1173,fv_1174,fv_1175,fv_1176,fv_1177,fv_1178,fv_1179,fv_1180,fv_1181,fv_1182,fv_1183,fv_1184,fv_1185,fv_1186,fv_1187,fv_1188,fv_1189,fv_1190,fv_1191,fv_1192,fv_1193,fv_1194,fv_1195,fv_1196,fv_1197,fv_1198,fv_1199,fv_1200,fv_1201,fv_1202,fv_1203,fv_1204,fv_1205,fv_1206,fv_1207,fv_1208,fv_1209,fv_1210,fv_1211,fv_1212,fv_1213,fv_1214,fv_1215,fv_1216,fv_1217,fv_1218,fv_1219,fv_1220,fv_1221,fv_1222,fv_1223,fv_1224,fv_1225,fv_1226,fv_1227,fv_1228,fv_1229,fv_1230,fv_1231,fv_1232,fv_1233,fv_1234,fv_1235,fv_1236,fv_1237,fv_1238,fv_1239,fv_1240,fv_1241,fv_1242,fv_1243,fv_1244,fv_1245,fv_1246,fv_1247,fv_1248,fv_1249,fv_1250,fv_1251,fv_1252,fv_1253,fv_1254,fv_1255,fv_1256,fv_1257,fv_1258,fv_1259,fv_1260,fv_1261,fv_1262,fv_1263,fv_1264,fv_1265,fv_1266,fv_1267,fv_1268,fv_1269,fv_1270,fv_1271,fv_1272,fv_1273,fv_1274,fv_1275,fv_1276,fv_1277,fv_1278,fv_1279,fv_1280,fv_1281,fv_1282,fv_1283,fv_1284,fv_1285,fv_1286,fv_1287,fv_1288,fv_1289,fv_1290,fv_1291,fv_1292,fv_1293,fv_1294,fv_1295,fv_1296,fv_1297,fv_1298,fv_1299,fv_1300,fv_1301,fv_1302,fv_1303,fv_1304,fv_1305,fv_1306,fv_1307,fv_1308,fv_1309,fv_1310,fv_1311,fv_1312,fv_1313,fv_1314,fv_1315,fv_1316,fv_1317,fv_1318,fv_1319,fv_1320,fv_1321,fv_1322,fv_1323,fv_1324,fv_1325,fv_1326,fv_1327,fv_1328,fv_1329,fv_1330,fv_1331,fv_1332,fv_1333,fv_1334,fv_1335,fv_1336,fv_1337,fv_1338,fv_1339,fv_1340,fv_1341,fv_1342,fv_1343,fv_1344,fv_1345,fv_1346,fv_1347,fv_1348,fv_1349,fv_1350,fv_1351,fv_1352,fv_1353,fv_1354,fv_1355,fv_1356,fv_1357,fv_1358,fv_1359,fv_1360,fv_1361,fv_1362,fv_1363,fv_1364,fv_1365,fv_1366,fv_1367,fv_1368,fv_1369,fv_1370,fv_1371,fv_1372,fv_1373,fv_1374,fv_1375,fv_1376,fv_1377,fv_1378,fv_1379,fv_1380,fv_1381,fv_1382,fv_1383,fv_1384,fv_1385,fv_1386,fv_1387,fv_1388,fv_1389,fv_1390,fv_1391,fv_1392,fv_1393,fv_1394,fv_1395,fv_1396,fv_1397,fv_1398,fv_1399,fv_1400,fv_1401,fv_1402,fv_1403,fv_1404,fv_1405,fv_1406,fv_1407,fv_1408,fv_1409,fv_1410,fv_1411,fv_1412,fv_1413,fv_1414,fv_1415,fv_1416,fv_1417,fv_1418,fv_1419,fv_1420,fv_1421,fv_1422,fv_1423,fv_1424,fv_1425,fv_1426,fv_1427,fv_1428,fv_1429,fv_1430,fv_1431,fv_1432,fv_1433,fv_1434,fv_1435,fv_1436,fv_1437,fv_1438,fv_1439,fv_1440,fv_1441,fv_1442,fv_1443,fv_1444,fv_1445,fv_1446,fv_1447,fv_1448,fv_1449,fv_1450,fv_1451,fv_1452,fv_1453,fv_1454,fv_1455,fv_1456,fv_1457,fv_1458,fv_1459,fv_1460,fv_1461,fv_1462,fv_1463,fv_1464,fv_1465,fv_1466,fv_1467,fv_1468,fv_1469,fv_1470,fv_1471,fv_1472,fv_1473,fv_1474,fv_1475,fv_1476,fv_1477,fv_1478,fv_1479,fv_1480,fv_1481,fv_1482,fv_1483,fv_1484,fv_1485,fv_1486,fv_1487,fv_1488,fv_1489,fv_1490,fv_1491,fv_1492,fv_1493,fv_1494,fv_1495,fv_1496,fv_1497,fv_1498,fv_1499,fv_1500,fv_1501,fv_1502,fv_1503,fv_1504,fv_1505,fv_1506,fv_1507,fv_1508,fv_1509,fv_1510,fv_1511,fv_1512,fv_1513,fv_1514,fv_1515,fv_1516,fv_1517,fv_1518,fv_1519,fv_1520,fv_1521,fv_1522,fv_1523,fv_1524,fv_1525,fv_1526,fv_1527,fv_1528,fv_1529,fv_1530,fv_1531,fv_1532,fv_1533,fv_1534,fv_1535,fv_1536,fv_1537,fv_1538,fv_1539,fv_1540,fv_1541,fv_1542,fv_1543,fv_1544,fv_1545,fv_1546,fv_1547,fv_1548,fv_1549,fv_1550,fv_1551,fv_1552,fv_1553,fv_1554,fv_1555,fv_1556,fv_1557,fv_1558,fv_1559,fv_1560,fv_1561,fv_1562,fv_1563,fv_1564,fv_1565,fv_1566,fv_1567,fv_1568,fv_1569,fv_1570,fv_1571,fv_1572,fv_1573,fv_1574,fv_1575,fv_1576,fv_1577,fv_1578,fv_1579,fv_1580,fv_1581,fv_1582,fv_1583,fv_1584,fv_1585,fv_1586,fv_1587,fv_1588,fv_1589,fv_1590,fv_1591,fv_1592,fv_1593,fv_1594,fv_1595,fv_1596,fv_1597,fv_1598,fv_1599,fv_1600,fv_1601,fv_1602,fv_1603,fv_1604,fv_1605,fv_1606,fv_1607,fv_1608,fv_1609,fv_1610,fv_1611,fv_1612,fv_1613,fv_1614,fv_1615,fv_1616,fv_1617,fv_1618,fv_1619,fv_1620,fv_1621,fv_1622,fv_1623,fv_1624,fv_1625,fv_1626,fv_1627,fv_1628,fv_1629,fv_1630,fv_1631,fv_1632,fv_1633,fv_1634,fv_1635,fv_1636,fv_1637,fv_1638,fv_1639,fv_1640,fv_1641,fv_1642,fv_1643,fv_1644,fv_1645,fv_1646,fv_1647,fv_1648,fv_1649,fv_1650,fv_1651,fv_1652,fv_1653,fv_1654,fv_1655,fv_1656,fv_1657,fv_1658,fv_1659,fv_1660,fv_1661,fv_1662,fv_1663,fv_1664,fv_1665,fv_1666,fv_1667,fv_1668,fv_1669,fv_1670,fv_1671,fv_1672,fv_1673,fv_1674,fv_1675,fv_1676,fv_1677,fv_1678,fv_1679,fv_1680,fv_1681,fv_1682,fv_1683,fv_1684,fv_1685,fv_1686,fv_1687,fv_1688,fv_1689,fv_1690,fv_1691,fv_1692,fv_1693,fv_1694,fv_1695,fv_1696,fv_1697,fv_1698,fv_1699,fv_1700,fv_1701,fv_1702,fv_1703,fv_1704,fv_1705,fv_1706,fv_1707,fv_1708,fv_1709,fv_1710,fv_1711,fv_1712,fv_1713,fv_1714,fv_1715,fv_1716,fv_1717,fv_1718,fv_1719,fv_1720,fv_1721,fv_1722,fv_1723,fv_1724,fv_1725,fv_1726,fv_1727,fv_1728,fv_1729,fv_1730,fv_1731,fv_1732,fv_1733,fv_1734,fv_1735,fv_1736,fv_1737,fv_1738,fv_1739,fv_1740,fv_1741,fv_1742,fv_1743,fv_1744,fv_1745,fv_1746,fv_1747,fv_1748,fv_1749,fv_1750,fv_1751,fv_1752,fv_1753,fv_1754,fv_1755,fv_1756,fv_1757,fv_1758,fv_1759,fv_1760,fv_1761,fv_1762,fv_1763,fv_1764,fv_1765,fv_1766,fv_1767,fv_1768,fv_1769,fv_1770,fv_1771,fv_1772,fv_1773,fv_1774,fv_1775,fv_1776,fv_1777,fv_1778,fv_1779,fv_1780,fv_1781,fv_1782,fv_1783,fv_1784,fv_1785,fv_1786,fv_1787,fv_1788,fv_1789,fv_1790,fv_1791,fv_1792,fv_1793,fv_1794,fv_1795,fv_1796,fv_1797,fv_1798,fv_1799,fv_1800,fv_1801,fv_1802,fv_1803,fv_1804,fv_1805,fv_1806,fv_1807,fv_1808,fv_1809,fv_1810,fv_1811,fv_1812,fv_1813,fv_1814,fv_1815,fv_1816,fv_1817,fv_1818,fv_1819,fv_1820,fv_1821,fv_1822,fv_1823,fv_1824,fv_1825,fv_1826,fv_1827,fv_1828,fv_1829,fv_1830,fv_1831,fv_1832,fv_1833,fv_1834,fv_1835,fv_1836,fv_1837,fv_1838,fv_1839,fv_1840,fv_1841,fv_1842,fv_1843,fv_1844,fv_1845,fv_1846,fv_1847,fv_1848,fv_1849,fv_1850,fv_1851,fv_1852,fv_1853,fv_1854,fv_1855,fv_1856,fv_1857,fv_1858,fv_1859,fv_1860,fv_1861,fv_1862,fv_1863,fv_1864,fv_1865,fv_1866,fv_1867,fv_1868,fv_1869,fv_1870,fv_1871,fv_1872,fv_1873,fv_1874,fv_1875,fv_1876,fv_1877,fv_1878,fv_1879,fv_1880,fv_1881,fv_1882,fv_1883,fv_1884,fv_1885,fv_1886,fv_1887,fv_1888,fv_1889,fv_1890,fv_1891,fv_1892,fv_1893,fv_1894,fv_1895,fv_1896,fv_1897,fv_1898,fv_1899,fv_1900,fv_1901,fv_1902,fv_1903,fv_1904,fv_1905,fv_1906,fv_1907,fv_1908,fv_1909,fv_1910,fv_1911,fv_1912,fv_1913,fv_1914,fv_1915,fv_1916,fv_1917,fv_1918,fv_1919,fv_1920,fv_1921,fv_1922,fv_1923,fv_1924,fv_1925,fv_1926,fv_1927,fv_1928,fv_1929,fv_1930,fv_1931,fv_1932,fv_1933,fv_1934,fv_1935,fv_1936,fv_1937,fv_1938,fv_1939,fv_1940,fv_1941,fv_1942,fv_1943,fv_1944,fv_1945,fv_1946,fv_1947,fv_1948,fv_1949,fv_1950,fv_1951,fv_1952,fv_1953,fv_1954,fv_1955,fv_1956,fv_1957,fv_1958,fv_1959,fv_1960,fv_1961,fv_1962,fv_1963,fv_1964,fv_1965,fv_1966,fv_1967,fv_1968,fv_1969,fv_1970,fv_1971,fv_1972,fv_1973,fv_1974,fv_1975,fv_1976,fv_1977,fv_1978,fv_1979,fv_1980,fv_1981,fv_1982,fv_1983,fv_1984,fv_1985,fv_1986,fv_1987,fv_1988,fv_1989,fv_1990,fv_1991,fv_1992,fv_1993,fv_1994,fv_1995,fv_1996,fv_1997,fv_1998,fv_1999,fv_2000,fv_2001,fv_2002,fv_2003,fv_2004,fv_2005,fv_2006,fv_2007,fv_2008,fv_2009,fv_2010,fv_2011,fv_2012,fv_2013,fv_2014,fv_2015,fv_2016,fv_2017,fv_2018,fv_2019,fv_2020,fv_2021,fv_2022,fv_2023,fv_2024,fv_2025,fv_2026,fv_2027,fv_2028,fv_2029,fv_2030,fv_2031,fv_2032,fv_2033,fv_2034,fv_2035,fv_2036,fv_2037,fv_2038,fv_2039,fv_2040,fv_2041,fv_2042,fv_2043,fv_2044,fv_2045,fv_2046,fv_2047,fv_2048,fv_2049,fv_2050,fv_2051,fv_2052,fv_2053,fv_2054,fv_2055,fv_2056,fv_2057,fv_2058,fv_2059,fv_2060,fv_2061,fv_2062,fv_2063,fv_2064,fv_2065,fv_2066,fv_2067,fv_2068,fv_2069,fv_2070,fv_2071,fv_2072,fv_2073,fv_2074,fv_2075,fv_2076,fv_2077,fv_2078,fv_2079,fv_2080,fv_2081,fv_2082,fv_2083,fv_2084,fv_2085,fv_2086,fv_2087,fv_2088,fv_2089,fv_2090,fv_2091,fv_2092,fv_2093,fv_2094,fv_2095,fv_2096,fv_2097,fv_2098,fv_2099,fv_2100,fv_2101,fv_2102,fv_2103,fv_2104,fv_2105,fv_2106,fv_2107,fv_2108,fv_2109,fv_2110,fv_2111,fv_2112,fv_2113,fv_2114,fv_2115,fv_2116,fv_2117,fv_2118,fv_2119,fv_2120,fv_2121,fv_2122,fv_2123,fv_2124,fv_2125,fv_2126,fv_2127,fv_2128,fv_2129,fv_2130,fv_2131,fv_2132,fv_2133,fv_2134,fv_2135,fv_2136,fv_2137,fv_2138,fv_2139,fv_2140,fv_2141,fv_2142,fv_2143,fv_2144,fv_2145,fv_2146,fv_2147,fv_2148,fv_2149,fv_2150,fv_2151,fv_2152,fv_2153,fv_2154,fv_2155,fv_2156,fv_2157,fv_2158,fv_2159,fv_2160,fv_2161,fv_2162,fv_2163,fv_2164,fv_2165,fv_2166,fv_2167,fv_2168,fv_2169,fv_2170,fv_2171,fv_2172,fv_2173,fv_2174,fv_2175,fv_2176,fv_2177,fv_2178,fv_2179,fv_2180,fv_2181,fv_2182,fv_2183,fv_2184,fv_2185,fv_2186,fv_2187,fv_2188,fv_2189,fv_2190,fv_2191,fv_2192,fv_2193,fv_2194,fv_2195,fv_2196,fv_2197,fv_2198,fv_2199,fv_2200,fv_2201,fv_2202,fv_2203,fv_2204,fv_2205,fv_2206,fv_2207,fv_2208,fv_2209,fv_2210,fv_2211,fv_2212,fv_2213,fv_2214,fv_2215,fv_2216,fv_2217,fv_2218,fv_2219,fv_2220,fv_2221,fv_2222,fv_2223,fv_2224,fv_2225,fv_2226,fv_2227,fv_2228,fv_2229,fv_2230,fv_2231,fv_2232,fv_2233,fv_2234,fv_2235,fv_2236,fv_2237,fv_2238,fv_2239,fv_2240,fv_2241,fv_2242,fv_2243,fv_2244,fv_2245,fv_2246,fv_2247,fv_2248,fv_2249,fv_2250,fv_2251,fv_2252,fv_2253,fv_2254,fv_2255,fv_2256,fv_2257,fv_2258,fv_2259,fv_2260,fv_2261,fv_2262,fv_2263,fv_2264,fv_2265,fv_2266,fv_2267,fv_2268,fv_2269,fv_2270,fv_2271,fv_2272,fv_2273,fv_2274,fv_2275,fv_2276,fv_2277,fv_2278,fv_2279,fv_2280,fv_2281,fv_2282,fv_2283,fv_2284,fv_2285,fv_2286,fv_2287,fv_2288,fv_2289,fv_2290,fv_2291,fv_2292,fv_2293,fv_2294,fv_2295,fv_2296,fv_2297,fv_2298,fv_2299,fv_2300,fv_2301,fv_2302,fv_2303,fv_2304,fv_2305,fv_2306,fv_2307,fv_2308,fv_2309,fv_2310,fv_2311,fv_2312,fv_2313,fv_2314,fv_2315,fv_2316,fv_2317,fv_2318,fv_2319,fv_2320,fv_2321,fv_2322,fv_2323,fv_2324,fv_2325,fv_2326,fv_2327,fv_2328,fv_2329,fv_2330,fv_2331,fv_2332,fv_2333,fv_2334,fv_2335,fv_2336,fv_2337,fv_2338,fv_2339,fv_2340,fv_2341,fv_2342,fv_2343,fv_2344,fv_2345,fv_2346,fv_2347,fv_2348,fv_2349,fv_2350,fv_2351,fv_2352,fv_2353,fv_2354,fv_2355,fv_2356,fv_2357,fv_2358,fv_2359,fv_2360,fv_2361,fv_2362,fv_2363,fv_2364,fv_2365,fv_2366,fv_2367,fv_2368,fv_2369,fv_2370,fv_2371,fv_2372,fv_2373,fv_2374,fv_2375,fv_2376,fv_2377,fv_2378,fv_2379,fv_2380,fv_2381,fv_2382,fv_2383,fv_2384,fv_2385,fv_2386,fv_2387,fv_2388,fv_2389,fv_2390,fv_2391,fv_2392,fv_2393,fv_2394,fv_2395,fv_2396,fv_2397,fv_2398,fv_2399,fv_2400,fv_2401,fv_2402,fv_2403,fv_2404,fv_2405,fv_2406,fv_2407,fv_2408,fv_2409,fv_2410,fv_2411,fv_2412,fv_2413,fv_2414,fv_2415,fv_2416,fv_2417,fv_2418,fv_2419,fv_2420,fv_2421,fv_2422,fv_2423,fv_2424,fv_2425,fv_2426,fv_2427,fv_2428,fv_2429,fv_2430,fv_2431,fv_2432,fv_2433,fv_2434,fv_2435,fv_2436,fv_2437,fv_2438,fv_2439,fv_2440,fv_2441,fv_2442,fv_2443,fv_2444,fv_2445,fv_2446,fv_2447,fv_2448,fv_2449,fv_2450,fv_2451,fv_2452,fv_2453,fv_2454,fv_2455,fv_2456,fv_2457,fv_2458,fv_2459,fv_2460,fv_2461,fv_2462,fv_2463,fv_2464,fv_2465,fv_2466,fv_2467,fv_2468,fv_2469,fv_2470,fv_2471,fv_2472,fv_2473,fv_2474,fv_2475,fv_2476,fv_2477,fv_2478,fv_2479,fv_2480,fv_2481,fv_2482,fv_2483,fv_2484,fv_2485,fv_2486,fv_2487,fv_2488,fv_2489,fv_2490,fv_2491,fv_2492,fv_2493,fv_2494,fv_2495,fv_2496,fv_2497,fv_2498,fv_2499,fv_2500,fv_2501,fv_2502,fv_2503,fv_2504,fv_2505,fv_2506,fv_2507,fv_2508,fv_2509,fv_2510,fv_2511,fv_2512,fv_2513,fv_2514,fv_2515,fv_2516,fv_2517,fv_2518,fv_2519,fv_2520,fv_2521,fv_2522,fv_2523,fv_2524,fv_2525,fv_2526,fv_2527,fv_2528,fv_2529,fv_2530,fv_2531,fv_2532,fv_2533,fv_2534,fv_2535,fv_2536,fv_2537,fv_2538,fv_2539,fv_2540,fv_2541,fv_2542,fv_2543,fv_2544,fv_2545,fv_2546,fv_2547,fv_2548,fv_2549,fv_2550,fv_2551,fv_2552,fv_2553,fv_2554,fv_2555,fv_2556,fv_2557,fv_2558,fv_2559,fv_2560,fv_2561,fv_2562,fv_2563,fv_2564,fv_2565,fv_2566,fv_2567,fv_2568,fv_2569,fv_2570,fv_2571,fv_2572,fv_2573,fv_2574,fv_2575,fv_2576,fv_2577,fv_2578,fv_2579,fv_2580,fv_2581,fv_2582,fv_2583,fv_2584,fv_2585,fv_2586,fv_2587,fv_2588,fv_2589,fv_2590,fv_2591,fv_2592,fv_2593,fv_2594,fv_2595,fv_2596,fv_2597,fv_2598,fv_2599,fv_2600,fv_2601,fv_2602,fv_2603,fv_2604,fv_2605,fv_2606,fv_2607,fv_2608,fv_2609,fv_2610,fv_2611,fv_2612,fv_2613,fv_2614,fv_2615,fv_2616,fv_2617,fv_2618,fv_2619,fv_2620,fv_2621,fv_2622,fv_2623,fv_2624,fv_2625,fv_2626,fv_2627,fv_2628,fv_2629,fv_2630,fv_2631,fv_2632,fv_2633,fv_2634,fv_2635,fv_2636,fv_2637,fv_2638,fv_2639,fv_2640,fv_2641,fv_2642,fv_2643,fv_2644,fv_2645,fv_2646,fv_2647,fv_2648,fv_2649,fv_2650,fv_2651,fv_2652,fv_2653,fv_2654,fv_2655,fv_2656,fv_2657,fv_2658,fv_2659,fv_2660,fv_2661,fv_2662,fv_2663,fv_2664,fv_2665,fv_2666,fv_2667,fv_2668,fv_2669,fv_2670,fv_2671,fv_2672,fv_2673,fv_2674,fv_2675,fv_2676,fv_2677,fv_2678,fv_2679,fv_2680,fv_2681,fv_2682,fv_2683,fv_2684,fv_2685,fv_2686,fv_2687,fv_2688,fv_2689,fv_2690,fv_2691,fv_2692,fv_2693,fv_2694,fv_2695,fv_2696,fv_2697,fv_2698,fv_2699,fv_2700,fv_2701,fv_2702,fv_2703,fv_2704,fv_2705,fv_2706,fv_2707,fv_2708,fv_2709,fv_2710,fv_2711,fv_2712,fv_2713,fv_2714,fv_2715,fv_2716,fv_2717,fv_2718,fv_2719,fv_2720,fv_2721,fv_2722,fv_2723,fv_2724,fv_2725,fv_2726,fv_2727,fv_2728,fv_2729,fv_2730,fv_2731,fv_2732,fv_2733,fv_2734,fv_2735,fv_2736,fv_2737,fv_2738,fv_2739,fv_2740,fv_2741,fv_2742,fv_2743,fv_2744,fv_2745,fv_2746,fv_2747,fv_2748,fv_2749,fv_2750,fv_2751,fv_2752,fv_2753,fv_2754,fv_2755,fv_2756,fv_2757,fv_2758,fv_2759,fv_2760,fv_2761,fv_2762,fv_2763,fv_2764,fv_2765,fv_2766,fv_2767,fv_2768,fv_2769,fv_2770,fv_2771,fv_2772,fv_2773,fv_2774,fv_2775,fv_2776,fv_2777,fv_2778,fv_2779,fv_2780,fv_2781,fv_2782,fv_2783,fv_2784,fv_2785,fv_2786,fv_2787,fv_2788,fv_2789,fv_2790,fv_2791,fv_2792,fv_2793,fv_2794,fv_2795,fv_2796,fv_2797,fv_2798,fv_2799,fv_2800,fv_2801,fv_2802,fv_2803,fv_2804,fv_2805,fv_2806,fv_2807,fv_2808,fv_2809,fv_2810,fv_2811,fv_2812,fv_2813,fv_2814,fv_2815,fv_2816,fv_2817,fv_2818,fv_2819,fv_2820,fv_2821,fv_2822,fv_2823,fv_2824,fv_2825,fv_2826,fv_2827,fv_2828,fv_2829,fv_2830,fv_2831,fv_2832,fv_2833,fv_2834,fv_2835,fv_2836,fv_2837,fv_2838,fv_2839,fv_2840,fv_2841,fv_2842,fv_2843,fv_2844,fv_2845,fv_2846,fv_2847,fv_2848,fv_2849,fv_2850,fv_2851,fv_2852,fv_2853,fv_2854,fv_2855,fv_2856,fv_2857,fv_2858,fv_2859,fv_2860,fv_2861,fv_2862,fv_2863,fv_2864,fv_2865,fv_2866,fv_2867,fv_2868,fv_2869,fv_2870,fv_2871,fv_2872,fv_2873,fv_2874,fv_2875,fv_2876,fv_2877,fv_2878,fv_2879,fv_2880,fv_2881,fv_2882,fv_2883,fv_2884,fv_2885,fv_2886,fv_2887,fv_2888,fv_2889,fv_2890,fv_2891,fv_2892,fv_2893,fv_2894,fv_2895,fv_2896,fv_2897,fv_2898,fv_2899,fv_2900,fv_2901,fv_2902,fv_2903,fv_2904,fv_2905,fv_2906,fv_2907,fv_2908,fv_2909,fv_2910,fv_2911,fv_2912,fv_2913,fv_2914,fv_2915,fv_2916,fv_2917,fv_2918,fv_2919,fv_2920,fv_2921,fv_2922,fv_2923,fv_2924,fv_2925,fv_2926,fv_2927,fv_2928,fv_2929,fv_2930,fv_2931,fv_2932,fv_2933,fv_2934,fv_2935,fv_2936,fv_2937,fv_2938,fv_2939,fv_2940,fv_2941,fv_2942,fv_2943,fv_2944,fv_2945,fv_2946,fv_2947,fv_2948,fv_2949,fv_2950,fv_2951,fv_2952,fv_2953,fv_2954,fv_2955,fv_2956,fv_2957,fv_2958,fv_2959,fv_2960,fv_2961,fv_2962,fv_2963,fv_2964,fv_2965,fv_2966,fv_2967,fv_2968,fv_2969,fv_2970,fv_2971,fv_2972,fv_2973,fv_2974,fv_2975,fv_2976,fv_2977,fv_2978,fv_2979,fv_2980,fv_2981,fv_2982,fv_2983,fv_2984,fv_2985,fv_2986,fv_2987,fv_2988,fv_2989,fv_2990,fv_2991,fv_2992,fv_2993,fv_2994,fv_2995,fv_2996,fv_2997,fv_2998,fv_2999,fv_3000";
    String columns2 = "id,name,gender,age,city";
    //conf.set(COLUMNS_NAMES_KEY, args[0]);
    conf.set(COLUMNS_NAMES_KEY, columns2);
    
    String tableName = args[0];
    Path inputDir = new Path(args[1]);

    String jobName = conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName);
    Job job = new Job(conf, jobName);
    job.setJarByClass(mapperClass);
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(mapperClass);
    job.setNumReduceTasks(0);
    new KuduTableMapReduceUtil.TableOutputFormatConfiguratorWithCommandLineParser(
        job,
        tableName)
        .configure();
    return job;
  }

  /*
   * @param errorMsg Error message. Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    String usage =
        "Usage: " + NAME + " <colAa,colB,colC> <table.name> <input.dir>\n\n" +
            "Imports the given input directory of CSV data into the specified table.\n" +
            "\n" +
            "The column names of the CSV data must be specified in the form of " +
            "comma-separated column names.\n" +
            "Other options that may be specified with -D include:\n" +
            "  -D" + SKIP_LINES_CONF_KEY + "=false - fail if encountering an invalid line\n" +
            "  '-D" + SEPARATOR_CONF_KEY + "=|' - eg separate on pipes instead of tabs\n" +
            "  -D" + JOB_NAME_CONF_KEY + "=jobName - use the specified mapreduce job name for the" +
            " import.\n" +
            CommandLineParser.getHelpSnippet();

    System.err.println(usage);
  }

  @Override
  public int run(String[] otherArgs) throws Exception {
    if (otherArgs.length < 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      return -1;
    }
    Job job = createSubmittableJob(getConf(), otherArgs);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int status = ToolRunner.run(new ImportCsv(), args);
    System.exit(status);
  }
}
