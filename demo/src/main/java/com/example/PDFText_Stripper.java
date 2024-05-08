package com.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.Connection;
import java.util.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.ArrayList;

import com.opencsv.CSVWriter;

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
/**
 * PDFText_Stripper
 */
public class PDFText_Stripper extends PDFTextStripper{
    public void PDFtoCSVConverter() {
        
//        Scanner sc=new Scanner(System.in);
        try {

            System.out.println("Fetching files from Path...");

            //File path is specified

            String currentPath = Paths.get("").toAbsolutePath().toString();

            File file=new File(currentPath+"/Files/JioInvoice_Final.pdf");

            System.out.println("Loading PDF...");

            //Loader loads pdf using filePath
            PDDocument pd=Loader.loadPDF(file);

            //Instantiation of textStripper class
            PDFTextStripper textStripper=new PDFTextStripper();

/*
            **************User choice for starting page***********

              * System.out.println("Enter page number to begin with");
              * int choice=Integer.parseInt(sc.nextLine());
              * sc.close();
              * textStripper.setStartPage(choice);
*/
            //Setting page limits
            textStripper.setStartPage(0);

            System.out.println("Stripping text out of PDF...");

            //Extracting text out of PDF
            String text=textStripper.getText(pd);

            //Creates new text file in the given location
            Path path=Paths.get(currentPath+"/Files/JioInvoice.txt");

            //Writing the extracted text to file 
            Files.writeString(path, text, StandardCharsets.UTF_8);
            System.out.println("Converting to text file...");

            //FileReader to read text file that has extracted data
            FileReader fr=new FileReader(currentPath+"/Files/JioInvoice.txt");
            BufferedReader br=new BufferedReader(fr);

            System.out.println("Finding right data...");
            System.out.println("Converting to csv file...");

            String filePath=currentPath+"/Files/Jio_Invoice_Data.csv";
            File txtFile=new File(filePath);

            SimpleDateFormat inputFormat = new SimpleDateFormat("dd-MMM-yy");
            SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");

            try
            {    
                FileWriter fw=new FileWriter(txtFile);
                CSVWriter cw=new CSVWriter(fw,',',CSVWriter.NO_QUOTE_CHARACTER,CSVWriter.DEFAULT_ESCAPE_CHARACTER,CSVWriter.DEFAULT_LINE_END);
                String line;
                List<String[]> data= new ArrayList<>();
                String[] rowHead={"No.","Start Date","Start Time","End Date","End Time","Destination","Total Usage(MB)","Billed Usage(MB)","Free Usage(MB)","Chargeable Usage(MB)","Amount"};
                data.add(rowHead);

                int count=0;
                String[] dbRowHead={"Serial_no","Start_date","Start_time","End_date","End_time","Destination","Total_usage_MB","Billed_usage_MB","Free_usage_MB","Chargable_usage_MB","Amount"};


                Properties login = new Properties();
                try (FileReader in = new FileReader(currentPath+"/login.properties")) {
                    login.load(in);
                }

                String username = login.getProperty("username");
                String password = login.getProperty("password");
                String url="jdbc:mysql://localhost:3306/myschema1";

                Connection connection=DriverManager.getConnection(url, username, password);
                System.out.println("Connected to DB successfully");

                //Read every line and find all strings that match with regex
                while((line=br.readLine())!=null)
                {
                    PatternMatcher patternMatcher = new PatternMatcher();
                    if(patternMatcher.getMatchedString(line))
                    {
                        String[] rowData=line.split(" ");
                        data.add(rowData);

                        StringBuilder queryBuilder=new StringBuilder();
                        
                        Date date = inputFormat.parse(rowData[1]);
                        rowData[1] = outputFormat.format(date);
                        Date date2 = inputFormat.parse(rowData[3]);
                        rowData[3] = outputFormat.format(date2);
                        System.out.println(rowData[1]+" "+rowData[3]); // Output: 2022-12-16
                        
                        queryBuilder.append("INSERT into jio_data (");
                        for(int i=0;i<dbRowHead.length;i++)
                        {
                            if (i < dbRowHead.length - 1) {
                                queryBuilder.append(dbRowHead[i]+", "); // Add a comma if not the last value
                            }
                        }
                        System.out.println(queryBuilder.toString());
                        queryBuilder.append(dbRowHead[dbRowHead.length-1]);
                        queryBuilder.append(") values (");
                        System.out.println(queryBuilder.toString());
                        for(int i=0;i<rowData.length;i++)
                        {
                            // queryBuilder.append(rowData[i]);
                            if(i!=1 || i!=3)
                            {
                                queryBuilder.append("?");
                                if(i<rowData.length-1)
                                {
                                    queryBuilder.append(", ");
                                }
                            }
                        }
                        // queryBuilder.append(rowData[rowData.length-1]);
                        queryBuilder.append(");");

                        System.out.println(queryBuilder.toString());
                        
                        PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.toString());
                        for(int i=0;i<rowData.length;i++)
                        {
                            preparedStatement.setString(i+1, rowData[i]);
                        }
                        System.out.println(preparedStatement);
                        preparedStatement.executeUpdate();
                        count++;
                    }
                }
                
                cw.writeAll(data);
                cw.close();
                connection.close();
                System.out.println(count + " rows inserted");
                System.out.println("Disconnected from DB");
                System.out.println("Completed.");
            }

            catch(Exception e)
            {
                e.printStackTrace();
            }

            fr.close();
            /*
            String txt_1=text.substring(5531, 5609);
            System.out.println("***********************Running*********************");
            PatternMatcher patternMatcher = new PatternMatcher();
            patternMatcher.getMatchedString(txt_1);
            System.out.println(txt_1);
            //Writing stripped text
            // System.out.println(text);
            */

            //Close document to prevent resource leakage
            pd.close();
        }

        catch (Exception e) {
            // Handle exception
            e.printStackTrace();
        }       
    }
}