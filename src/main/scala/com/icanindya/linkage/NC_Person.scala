package com.icanindya.linkage

class NC_Person(val first_name: String, val middle_name: String, val last_name: String, val sex: String, val race: String, val ethnicity: String, val age: String, val birth_place: String, val zip: String, val county: String, val party: String, val regDate: String, val phone: String, val voterNum: String, val ncId: String, val address: String) extends Serializable{
  def sanitizedPhone(digits: Int): String = if(digits <= phone.length()) phone.substring(phone.length() - digits) else phone
}