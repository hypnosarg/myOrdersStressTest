import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

public class DateUtils {
    public static final int LOW_DATE_YEAR = 1;
    protected static String CDATESEPARATOR = "/";
    protected static String CTIMESEPARATOR = ":";

    public static String dateToOutputDDMMHHMM(String inLdt){
        return inLdt.substring(6,8) + CDATESEPARATOR + inLdt.substring(4,6) + " "
                                                                + inLdt.substring(8,10) + CTIMESEPARATOR
                                                                + inLdt.substring(10,12) ;
    }

    public static String dateToOutputDDMM(String inDate){
        return inDate.substring(8,10) + CDATESEPARATOR + inDate.substring(5,7);
    }
    public static LocalDateTime dateStringToDate( String dateStr){
        int year     = Integer.parseInt( dateStr.substring(0,4) );
        int month    = Integer.parseInt( dateStr.substring(5,7) );
        int day      = Integer.parseInt( dateStr.substring(8,10) );

        return LocalDateTime.of(year,month,day,00,00,00);
    }

    public static LocalDateTime dateYYYYMMDDHHMMSSToLdt( String dateStr){
        //Parse the date as String into integers
        if (dateStr == null || dateStr.isEmpty() || dateStr.charAt(0) == ' '){
            //Empty input? then return a very low date
            return getLowDate();
        }
        String trimmed = dateStr.trim();
        int year     = Integer.parseInt( trimmed.substring(0,4) );
        int month    = Integer.parseInt( trimmed.substring(4,6) );
        int day      = Integer.parseInt( trimmed.substring(6,8) );
        int hour     = Integer.parseInt( trimmed.substring(8,10) );
        int minutes  = Integer.parseInt( trimmed.substring(10,12) );
        int seconds  = Integer.parseInt( trimmed.substring(12) );

        return LocalDateTime.of(year,month,day,hour,minutes,seconds);
    }
    public static Date dateYYYYMMDDHHMMSSToDate( String dateStr) {
        return Date.from(dateYYYYMMDDHHMMSSToLdt(dateStr).toInstant(ZoneOffset.UTC));
    }

    public static String dateToEdmDateStr(Date inDate){
        String firstPartOfPattern = "yyyy-MM-dd";
        String secondPartOfPattern = "HH:mm:ss";
        SimpleDateFormat sdf1 = new SimpleDateFormat(firstPartOfPattern);
        SimpleDateFormat sdf2 = new SimpleDateFormat(secondPartOfPattern);
        return "datetime"+"'"+sdf1.format(inDate) + "T" + sdf2.format(inDate).replace(":","%3A")+"'";
    }


    public static LocalDateTime getLocalDateTimeFromInts(int year, int month, int day, int hour,int minute){
       return LocalDateTime.of(year,month,day,hour,minute,00);
    }



    public static int getCurrentWeek(){
        Calendar calendar = new GregorianCalendar();
        Date trialTime = new Date();
        calendar.setTime(trialTime);
        return calendar.get(Calendar.WEEK_OF_YEAR);
    }


    public static LocalDateTime getNowLdt(){
        Calendar today = Calendar.getInstance();
        return LocalDateTime.of(today.get(Calendar.YEAR),today.get(Calendar.MONTH) + 1,today.get(Calendar.DAY_OF_MONTH),
                                 today.get(Calendar.HOUR_OF_DAY),today.get(Calendar.MINUTE),today.get(Calendar.SECOND));
    }

    public static LocalDateTime getNowLdtUTC(){
            return getCurrentTime("UTC");
    }

    public static LocalDateTime getCurrentTime(String timeZone){
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.ENGLISH);
        f.setTimeZone(TimeZone.getTimeZone(timeZone));
        String zonedDateTime = f.format(new Date());
        zonedDateTime = zonedDateTime.replace(" ","T");
        return LocalDateTime.parse(zonedDateTime);

    }

    public static LocalDateTime getCurrentTimeBe(){
        return getCurrentTime("CET");
    }


    public static  LocalDateTime getLowDate(){
        return LocalDateTime.of(LOW_DATE_YEAR,1,1,1,1,1);
    }
}
