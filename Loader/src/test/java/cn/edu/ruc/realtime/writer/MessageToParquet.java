//package cn.edu.ruc.realtime.writer;
//
///**
// * RealTimeAnalysis
// *
// * @author Jelly
// */
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.parquet.example.data.Group;
//import org.apache.parquet.example.data.simple.SimpleGroupFactory;
//import org.apache.parquet.hadoop.example.GroupWriteSupport;
//import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//import org.apache.parquet.io.api.Binary;
//import org.apache.parquet.schema.MessageType;
//import org.apache.parquet.schema.PrimitiveType;
//import org.apache.parquet.schema.Type;
//import shaded.parquet.org.codehaus.jackson.JsonNode;
//import shaded.parquet.org.codehaus.jackson.map.ObjectMapper;
//
//import java.io.IOException;
//import java.util.*;
//
//import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
//
///**
// * Created by Richard on 2016-07-18.
// */
//public class MessageToParquet{
//    protected Queue queue;
//    protected MessageType schema;
//    protected SimpleGroupFactory factory;
//    private static final String TAB ="\t";
//    protected ArrayList<PathAction> recorder;
//    protected ParquetWriter realWriter;
//    /*realWriter =new ParquetWriter(file,
//                    writeSupport, CompressionCodecName.GZIP, blockSize, pageSize);*/
//    public MessageToParquet(MessageType schema) throws IOException {
//
// /*      // this.queue =queue;Queue queue,
//        Configuration conf = new Configuration();
//        GroupWriteSupport writeSupport = new GroupWriteSupport();
//        writeSupport.setSchema(schema, conf);
//        Path file = new Path("target/tests/TestParquetWriter/");
//        //File file2 = new File("target/tests/TestParquetWriter/");
//        int blockSize = 256 * 1024 * 1024;
//        int pageSize = 64 * 1024;
//        System.out.println(schema+"aaaaaaaaaaaaaaaaaaaaaaaaaaaa");
//        realWriter =new ParquetWriter(file,
//                writeSupport, CompressionCodecName.SNAPPY, blockSize, pageSize);
//new ParquetWriter<Group>(file, writeSupport,
//                ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
//                ParquetWriter.DEFAULT_BLOCK_SIZE,
//                ParquetWriter.DEFAULT_PAGE_SIZE,
//                ParquetWriter.DEFAULT_PAGE_SIZE, // dictionary page size
//                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
//                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
//                ParquetProperties.WriterVersion.PARQUET_1_0,
//                conf
//        );*/
//    }
//    public static void main(String[] args) {
//        MessageType schema = parseMessageType(
//                "message test { "
//                        + "required binary binary_field; "
//                        + "repeated int32 int32_field; "
//                        + "optional int64 int64_field; "
//                        + "required boolean boolean_field; "
//                        + "required float float_field; "
//                        + "required double double_field; "
//                        + "repeated group contacts_field {\n" +
//                        "   required binary name;\n" +
//                        "   optional binary phoneNumber;\n" +
//                        " }"
//                        + "required fixed_len_byte_array(3) flba_field; "
//                        + "required int96 int96_field; "
//                        + "} ");
//        //System.out.println(schema);
//        //Queue q = new Queue();
//        try {
//            MessageToParquet messageToParquet = new MessageToParquet(schema);
//            //System.out.println(schema);
//            messageToParquet.write(schema);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public boolean write(MessageType schema) throws IOException {
//        // this.queue =queue;Queue queue,
//        Configuration conf = new Configuration();
//        GroupWriteSupport writeSupport = new GroupWriteSupport();
//        writeSupport.setSchema(schema, conf);
//        Path file = new Path("target/tests/TestParquetWriter/");
//        //File file2 = new File("target/tests/TestParquetWriter/");
//        int blockSize = 256 * 1024 * 1024;
//        int pageSize = 64 * 1024;
//
//        //System.out.println(schema+"aaaaaaaaaaaaaaaaaaaaaaaaaaaa");
//        factory = new SimpleGroupFactory(schema);
//        recorder = new ArrayList<PathAction>();
//
//
//
//        ArrayList<String[]> Paths = (ArrayList)schema.getPaths();
//        Iterator<String[]> pi = Paths.listIterator();
//
//        String[] prevPath = {};
//
//        while (pi.hasNext()) {
//
//            String p[] = pi.next();
//
//            // Find longest common path between prev_path and current
//            ArrayList<String> commonPath = new ArrayList<String>();
////            int m = 0;
//            for (int n = 0; n < prevPath.length; n++) {
//                if (n < p.length && p[n].equals(prevPath[n])) {
//                    commonPath.add(p[n]);
//                } else
//                    break;
//            }
//
//            // If current element is not inside previous group, restore to the group of common path
//            for (int n = commonPath.size(); n < prevPath.length - 1; n++)
//                recorder.add(new PathAction(PathAction.ActionType.GROUPEND));
//
//            // If current element is not right after common path, create all required groups
//            for (int n = commonPath.size(); n < p.length - 1; n++) {
//                PathAction a = new PathAction(PathAction.ActionType.GROUPSTART);
//                a.setName(p[n]);
//                recorder.add(a);
//            }
//
//            prevPath = p;
//
//            PathAction as = new PathAction(PathAction.ActionType.FIELD);
//
//            Type colType = schema.getType(p);
//
//            as.setType(colType.asPrimitiveType().getPrimitiveTypeName());
//            as.setRepetition(colType.getRepetition());
//            as.setName(p[p.length - 1]);
//
//            recorder.add(as);
//        }
//
//
//
//        //Queue queue,
//        String key ="binary_field|int32_field|int64_field|boolean_field|"
//                +"float_field|double_field|name|phoneNumber|name|phoneNumber|flba_field|int96_field";//"0123456|0928171|1222|1111|11112";
//        String value ="1|2|3|4|5|6|{'contacts_field':{'name':7;'phoneNumber':8;}};'contacts_field':{'name':7;'phoneNumber':8;}}|9|13|11|12";
//        Group grp = factory.newGroup();
//        String[] strK = key.toString().split("\\|",-1);
//        String[] strV = value.toString().split("\\|",-1);
//        String kv_combined[] = strV;
//        //String kv_combined[] = (String[]) ArrayUtils.addAll(strK, strV);
//        Iterator<PathAction> ai = recorder.iterator();
//        System.out.println(recorder.toString());
//        Stack<Group> groupStack = new Stack<Group>();
//        groupStack.push(grp);
//        int i = 0;
//
//        while(ai.hasNext()) {
//
//            PathAction a = ai.next();
//
//            switch (a.getAction()) {
//                case GROUPEND:
//                    grp = groupStack.pop();
//                    System.out.println("GROUPEND"+a.getName());
//                    break;
//
//                case GROUPSTART:
//                    groupStack.push(grp);
//                    grp = grp.addGroup(a.getName());
//                    System.out.println("GROUPSTART"+a.getName());
//                    break;
//
//                case FIELD:
//                    System.out.println(a.getName()+a.getType()+a.getAction()+a.getRepetition());
//                    String s;
//                    PrimitiveType.PrimitiveTypeName primType = a.getType();
//                    String colName = a.getName();
//                    if (i < kv_combined.length) {
//                        //System.out.println(i);
//                        s = kv_combined[i ++];
//                        // System.out.println(s+"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa"+i);
//                    } else {
//                        if (a.getRepetition() == Type.Repetition.OPTIONAL) {
//                            i ++;
//                            continue;
//                        }
//                        s = primType == PrimitiveType.PrimitiveTypeName.BINARY ? "" : "0";
//                        // System.out.println(s+"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"+i);
//                    }
//
//                    // If we have 'repeated' field, assume that we should expect JSON-encoded array
//                    // Convert array and append all values
//                    int repetition = 1;
//                    boolean repeated = false;
//                    ArrayList<String> s_vals = null;
//                    //System.out.println(s+"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
//                    if (a.getRepetition() == Type.Repetition.REPEATED) {
//                        repeated = true;
//                        s_vals = new ArrayList<String>();
//                        ObjectMapper mapper = new ObjectMapper();
//                        System.out.println(s+"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
//                        JsonNode node = mapper.readTree(s);
//                        Iterator <JsonNode> itr = node.iterator();
//                        for(int g=0;g<node.size();g++){
//                            System.out.println(node.get(g).getTextValue());
//                        }
//                        repetition = 0;
//                        while(itr.hasNext()) {
//                            s_vals.add(itr.next().getTextValue());  // No array-of-objects!
//                            repetition ++;
//                        }
//                    }
//                    System.out.println("repetition="+repetition);
//                    for (int j = 0; j < repetition; j ++) {
//
//                        if (repeated)
//                            // extract new s
//                            s = s_vals.get(j);
//
//                        System.err.println(primType);
//                        try {
//                            switch (primType) {
//
//                                case INT32:
//                                    System.out.println(s+"32aaaaaaaaaaa");
//                                    grp.append(colName, Integer.parseInt(s));
//
//                                    break;
//                                case INT64:
//                                case INT96:
//                                    grp.append(colName, Long.parseLong(s));
//                                    System.out.println(s+"96aaaaaaaa");
//                                    break;
//                                case DOUBLE:
//                                    grp.append(colName, Double.parseDouble(s));
//                                    System.out.println(s+"doubleaaa");
//                                    break;
//                                case FLOAT:
//                                    grp.append(colName, Float.parseFloat(s));
//                                    System.out.println(s+"floataaaaaaa");
//                                    break;
//                                case BOOLEAN:
//                                    grp.append(colName, s.equals("true") || s.equals("1"));
//                                    System.out.println(s+"booleanaaaaaaaaaaaa");
//                                    break;
//                                case BINARY:
//                                    grp.append(colName, Binary.fromString(s));
//                                    System.out.println(s+"binaryaaaaaaaaaaaaa");
//                                    break;
//                                case FIXED_LEN_BYTE_ARRAY:
//                                    grp.append(colName, Binary.fromString(s));
//                                    System.out.println(s+"fixedaaaaaaaaaaa");
//                                    break;
//                                default:
//                                    throw new RuntimeException("Can't handle type " + primType);
//                            }
//                        } catch (NumberFormatException e) {
//
//                            grp.append(colName, 0);
//                        }
//                    }
//                    System.out.println("filed"+a.getName());
//            }
//        }
//        System.out.println("aaxa"+grp.getFieldRepetitionCount("contacts_field"));
//        realWriter =new ParquetWriter(file,
//                writeSupport, CompressionCodecName.UNCOMPRESSED, blockSize, pageSize);
//        realWriter.write(grp);
//        return false;
//    }
//
//
//    private void enforceEmptyDir(Configuration conf, Path path) throws IOException {
//
//        FileSystem fileSystem = path.getFileSystem(conf);
//
//        if (fileSystem.exists(path)) {
//            if (!fileSystem.delete(path, true)) {
//                throw new IOException("can not delete path" + path);
//            }
//        }
//        if (!fileSystem.mkdirs(path)) {
//            throw new IOException("can not create path " + path);
//        }
//    }
//}
//
