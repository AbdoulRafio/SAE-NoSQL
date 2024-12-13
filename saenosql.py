#### SAE NOSQL ####

#----------------------------#
# Nourelhouda DJERIOU
# Abdoul Rafio BANGAGNE
# Mohamed Amir BELARBI
#----------------------------#


# Importation des modules utilisés
import sqlite3
import pandas

# Création de la connexion
conn = sqlite3.connect("ClassicModel.sqlite")

# # Récupération du contenu de Customers avec une requête SQL
# customers = pandas.read_sql_query("SELECT * FROM Customers;", conn)
# print(customers)



# 1. Lister les clients n’ayant jamais effecuté une commande
q1 = pandas.read_sql_query("""
    SELECT Customers.customerNumber, Customers.customerName  FROM Customers 
    LEFT JOIN Orders 
    ON Customers.customerNumber = Orders.customerNumber
    WHERE Orders.customerNumber IS NULL;
    """, conn)
# print(q1)



#2. Pour chaque employé, le nombre de clients, le nombre de commandes et le montant total de celles-ci ;
q2 = pandas.read_sql_query("""
    SELECT 
        e.employeeNumber,
        e.firstName,
        e.lastName,
        count(distinct(c.customerNumber)) AS Nombre_de_Client, 
        count(distinct(o.orderNumber)) AS Nombre_commande, 
        sum(p.amount) AS Montanttot_Paiement
    FROM Employees e 
    LEFT JOIN Customers c On e.employeeNumber = c.salesRepEmployeeNumber
    LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
    LEFT JOIN Payments p ON o.customerNumber = p.customerNumber
    GROUP BY e.employeeNumber,e.lastName,e.firstName
    ;
    """, conn)
# print(q2)




#3. Idem pour chaque bureau (nombre de clients, nombre de commandes et montant total), avec en plus le nombre de clients d’un pays différent, s’il y en a ;
q3 = pandas.read_sql_query("""
    SELECT 
        oi.officeCode, 
        oi.city, 
        oi.country AS officeCountry,
        COUNT(DISTINCT(c.customerNumber)) AS Nombre_de_Client, 
        COUNT(DISTINCT(o.orderNumber)) AS Nombre_commande, 
        SUM(p.amount) AS Montanttot_Paiement,
        COUNT(DISTINCT CASE WHEN c.country != oi.country THEN c.customerNumber ELSE NULL END) AS CustomerFromdifferentCountry
    FROM Employees e 
    LEFT JOIN Offices oi ON e.officeCode = oi.officeCode
    LEFT JOIN Customers c On e.employeeNumber = c.salesRepEmployeeNumber
    LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
    LEFT JOIN Payments p ON o.customerNumber = p.customerNumber
    GROUP BY oi.officeCode,oi.city,oi.country
    ;
    """, conn)
# print(q3)



#4. Pour chaque produit, donner le nombre de commandes, la quantité totale commandée, et le nombre de clients différents ;
q4 = pandas.read_sql_query(
    """ 
    SELECT 
        p.productCode, 
        p.productName, 
        COUNT (DISTINCT od.orderNumber) AS numberoforders, 
        SUM (od.quantityordered) AS totalQuantityordered, 
        COUNT (DISTINCT o.customerNumber) AS numberofDistinctCustomers 
    FROM Products p 
    LEFT JOIN OrderDetails od ON p.productCode = od.productCode 
    LEFT JOIN Orders o ON od.orderNumber = o.orderNumber 
    GROUP BY p.productCode, p.productName
    ;
    """, conn)
# print(q4)



#5. Donner le nombre de commande pour chaque pays du client, ainsi que le montant total des commandes et le montant total payé : on veut conserver les clients n’ayant jamais commandé dans le résultat final ;
q5 = pandas.read_sql_query("""SELECT c.country, 
       COUNT(DISTINCT o.orderNumber) AS nombreCommandes, 
       SUM(od.quantityOrdered * od.priceEach) AS MontantTotalCommandes,
       SUM(p.amount) AS MontantsTotalPayés
    FROM Customers c
    LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
    LEFT JOIN OrderDetails od ON o.orderNumber = od.orderNumber
    LEFT JOIN Payments p ON c.customerNumber = p.customerNumber
    GROUP BY c.country;""", conn)
# print(q5)



#6. On veut la table de contigence du nombre de commande entre la ligne de produits et le pays du client ;
q6 = pandas.read_sql_query("""SELECT 
                    Products.productLine,
                    Customers.country,
                    COUNT(DISTINCT Orders.orderNumber) AS nombre_commandes
                FROM 
                    Products
                    LEFT JOIN OrderDetails ON Products.productCode =  OrderDetails.productCode
                    LEFT JOIN Orders ON OrderDetails.orderNumber  = Orders.orderNumber
                    LEFT JOIN Customers ON Orders.customerNumber = Customers.customerNumber
                           
                           
                GROUP BY 
                    Products.productLine,
                    Customers.country;

                                       """, conn)
# print(q6)



#7. On veut la même table croisant la ligne de produits et le pays du client, mais avec le montant total payé dans chaque cellule ;
q7 = pandas.read_sql_query("""SELECT 
                    Products.productLine,
                    Customers.country,
                    SUM(Payments.amount) AS montant_total_payé
                FROM 
                    Products
                    LEFT JOIN OrderDetails ON Products.productCode =  OrderDetails.productCode
                    LEFT JOIN Orders ON OrderDetails.orderNumber  = Orders.orderNumber
                    LEFT JOIN Customers ON Orders.customerNumber = Customers.customerNumber
                    LEFT JOIN Payments ON Customers.customerNumber = Payments.customerNumber
                GROUP BY 
                    Products.productLine,
                    Customers.country;

                                        """, conn)
# print(q7)




#8. Donner les 10 produits pour lesquels la marge moyenne est la plus importante (cf buyPrice et priceEach) ;
q8 = pandas.read_sql_query("""SELECT 
        p.productCode, 
        p.productName, 
       AVG(od.priceEach - p.buyPrice) AS Marge_moyenne
    FROM Products p
    JOIN OrderDetails od ON p.productCode = od.productCode
    GROUP BY p.productCode
    ORDER BY Marge_moyenne DESC
    LIMIT 10;""", conn)
# print(q8)




#9. Lister les produits (avec le nom et le code du client) qui ont été vendus à perte :
    #- Si un produit a été dans cette situation plusieurs fois, il doit apparaître plusieurs fois,
    #- Une vente à perte arrive quand le prix de vente est inférieur au prix d’achat ;
q9 = pandas.read_sql_query("""SELECT 
        p.productCode, 
        p.productName, 
        c.customerName,
        c.customerNumber, 
        od.priceEach, 
        p.buyPrice
    FROM OrderDetails od
    JOIN Products p ON od.productCode = p.productCode
    JOIN Orders o ON od.orderNumber = o.orderNumber
    JOIN Customers c ON o.customerNumber = c.customerNumber
    WHERE od.priceEach < p.buyPrice;""", conn) 
# print(q9)



#10. (bonus) Lister les clients pour lesquels le montant total payé est inférieur aux montants totals des achats ;
q10 = pandas.read_sql_query("""SELECT 
                    Customers.customerName,
                    SUM(Payments.amount)  AS MontanttotAchatSupérieur ,
                    SUM(OrderDetails.priceEach * OrderDetails.quantityOrdered)  AS MontanttotPayésInférieur
                FROM 
                    OrderDetails
                    LEFT JOIN Orders ON OrderDetails.orderNumber  = Orders.orderNumber
                    LEFT JOIN Customers ON Orders.customerNumber = Customers.customerNumber
                    LEFT JOIN Payments ON Customers.customerNumber = Payments.customerNumber
                    GROUP BY 
    Customers.customerName
HAVING 
    SUM(Payments.amount) > SUM(OrderDetails.priceEach * OrderDetails.quantityOrdered);
        

                                        """, conn)
# print(q10)



# Fermeture de la connexion : IMPORTANT à faire dans un cadre professionnel
conn.close()




### Méthode clé valeur : Avantages : données géographiques, gestion des commandes, facilité de recherche

##Diapo 47 à voir