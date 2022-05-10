using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace ParseDB;

public class XmlVaxRecord
{
    public Site site { get; set; }

    [XmlArrayItem("brand")]
    public XmlVaccines[] vaccines
    {
        get;
        set;
    }


    [XmlAttribute]
    public int year { get; set; }

    [XmlAttribute]
    public int month { get; set; }

    [XmlAttribute]
    public int day { get; set; }
}

public class VaxRecord
{
    public Site site
    {
        get;
        set;
    }
    
    public Date date
    {
        get;
        set;
    }
    
    public Vaccines[] vaccines
    {
        get;
        set;
    }

}