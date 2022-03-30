resource "azurerm_resource_group" "rg" {
  name     = "rgatividadefinal"
  location = var.location
  tags = {
    Student      = "Fillipe Goulart"
    Student_Code = "${var.student_code}"
    E-mail       = "fillipe.gsm@tutanota.com"
    Environment  = "Trabalho Final"
  }
}
