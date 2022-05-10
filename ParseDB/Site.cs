using System.Xml.Serialization;

namespace ParseDB;

public class Site
{
    [XmlAttribute]
    public String id { get; set; }
    public String name { get; set; }
    public String zipCode { get; set; }
}